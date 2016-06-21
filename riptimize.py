"""
This module provides the function riptimize() that is intended to
optimize AWS Reserved Instance (RI) utilization by redistributing RIs
across the typically 3 availability zones (AZs) in a single RI holding
account. The premise of the script is that there exist many AWS accounts
linked via consolidated billing in which on-demand instances are being
launched. One special account is designated as an RI holding account
where all RIs are purchased. In a consolidated billing setup the RIs
reserved in one account can actually be "used" by an instance launced in
another account, therefore, it is sufficient to keep all RIs for
simplicity in one account. It is also assumed that all linked accounts
have already remapped their logical AZs to correspond to the same
physical datacenters, and that only 3 AZs are being used. If some
accounts have running instances in other AZs, a recommendation is issued
to migrate them to one of the 3 supported AZs. If a corresponding option
('optimize') is selected, the function will actually perform needed
modifications in order to migrate the RIs and thus increase RI
utilization.
"""
import time
import boto
import boto.ec2

# TODO Idea: publish RI utilization metrics to CloudWatch so they can be
#   viewed in the console
# TODO Idea: save results of script execution into an S3 bucket for
#   logging purposes

def riptimize(all_accounts, ri_account_credentials, region, optimize=False,
              publish_metrics=True):
    """
    riptimize() accepts the following arguments:

    all_accounts -- a dict carriying the account credentials of all accounts
                            in the consolidated account tree that need to be
                            taken into consideration. The key is some account
                            identifier, typically the 12 digit AWS Account ID.
                            The value is a tuple of Access Key ID and Secret
                            Access Key.

    ri_account_credentials -- a tuple containing the account credentials for the
                            RI holding account

    region -- the function is designed to be used in one specific region, e.g.
                           'us-east-1' specified in this parameter

    optimize -- When True the optimization modifications (if any) will be
                            executed for real, otherwise they will execute in a
                            DRY-RUN mode. This is an optional parameter, which
                            is False by default

    publish_metrics -- When False, do not publish RI usage metrics to CloudWatch,
                            True by default

    and returns a large tuple consisting of the following information:

    i_inventory -- a dict keyed by a tuple (instance_type, availability_zone),
                            with the value being the count of running on-demand instances

    i_inventory_by_account -- a dict keyed by the account_id where the values
                            are the same as i_inventory above

    ri_inventory -- same as i_inventory but for RIs in the RI holding account,
                            RIs in other accounts are ignored

    supported_ri_zones -- a list of availability zones used by the RI holding
                            account, RIs can only move between these zones

    processing_modifications -- a list of modification_ids of all previous RI
                            modifications that are still in 'processing' state

    clean_mismatch -- a dict similar to i_inventory, except values are count
                            differences between the corresponding values of
                            ri_inventory and i_inventory, i.e. negative
                            values mean that RIs are needed for the
                            corresponding combination of instance type and
                            availability zone.

    recommendations -- a tuple containing two elements, each corresponding to
                            details of two different recommendations: the
                            first being the sub-inventory of on-demand
                            instances running in unsupported availability
                            zones and the second being the overall RI
                            surplus (or deficit) for each instance type
                            aggregated over all availability zones

    plan -- if RIs can be redistributed for optimization reasons this will
                            contain the the high-level RI modification
                            plan. It is a list of tuples, each of which
                            contains the instance type, the source AZ, the
                            destination AZ and the number of RIs that need
                            to be moved from source AZ to destination AZ

    modification_ids -- Once the plan is translated into specific RI
                            modifications and those modifications are
                            kicked off, this list will contain the
                            modification IDs returned by
                            ModifyReservedInstances API, so these can be
                            tracked later on
    """
    # 1. get the inventory for on-demand instances running in all linked accounts
    i_inventory_by_account = get_i_inventory_by_account(all_accounts, region)
    i_inventory = aggregate_inventory(i_inventory_by_account)

    # 2. get the RI inventory in the RI holding account, supported RI zones and any previous RI modifications that are still being processed
    ri_inventory, supported_ri_zones, processing_modifications = get_ri_inventory(ri_account_credentials, region)
    modifications_inflight = len(processing_modifications) != 0

    # 3. compute On-demand/RI inventory mismatches per availability zone
    mismatch = compute_ri_mistmatch(ri_inventory, i_inventory)

    # 4. get rid of mismatches in zones that RIs do not cover in the RI holding account
    clean_mismatch, eliminated_i_inventory = eliminate_unsupported_zones(mismatch, supported_ri_zones)

    # 5. figure out what the RI surplus (or deficit) is for each instance type across all linked accounts
    ri_surplus = compute_ri_surplus(clean_mismatch)
    # get rid of entries where RIs and running instances are perfectly balanced
    ri_imbalance = {itype: diff for itype, diff in ri_surplus.items() if diff != 0}

    # 6. create recommendations for migrating instances to supported zones, purchasing more RIs and/or starting more instances
    recommendations = (eliminated_i_inventory, ri_imbalance)

    # 7. if an RI distributions are possible that would optimize RI utilization, generate a modification plan
    modification_ids = []
    # for now generate a "greedy" plan. Eventually, a smarter plan can be created, e.g. the one that minimizes modifications
    plan = greedy_distribution(clean_mismatch)
    if len(plan) > 0:
        perform_optimization = optimize and not modifications_inflight
        # 8. execute the plan either for real or in a DRY-RUN mode
        modification_ids = execute_plan(ri_account_credentials, region, plan, perform_optimization)

    # 9. publish RI usage metrics to CloudWatch
    if publish_metrics:
        # TODO also publish RI utilization metrics, % of utilization
        publish_cw_metrics(ri_account_credentials, region, ri_surplus)

    # 10. finally, return all the collected information for generation of reports, logging, etc.
    return (i_inventory, i_inventory_by_account, ri_inventory, supported_ri_zones, processing_modifications, clean_mismatch, recommendations, plan, modification_ids)

    # TODO The function currently simply kicks off the proposed mofications without verifying whether they actually succeeded. Since the kinds of modifications
    # performed by this script are not likely to fail (no new RIs are purchased in the process), the modifications are extremely unlikely to fail, but nonetheless
    # monitoring the success of such modifications would be a recommended addition to the logic


def get_i_inventory_by_account(all_accounts, region):
    inventory_by_account = {}
    for account_id, credentials in all_accounts.items():
        inventory_by_account[account_id] = get_account_i_inventory(credentials, region)

    return inventory_by_account


def get_account_i_inventory(credentials, region):
    access_key_id, secret_access_key = credentials
    conn = boto.ec2.connect_to_region(region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    account_inventory = {}

    # TODO should instances that are launching at this very moment be included in this report? Probably...
    filters = {'instance-state-name' : 'running'}
    for instance in conn.get_only_instances(filters=filters):
        itype_and_az = instance.instance_type, instance.placement
        if itype_and_az in account_inventory:
            account_inventory[itype_and_az] += 1
        else:
            account_inventory[itype_and_az] = 1

    conn.close()
    return account_inventory


def aggregate_inventory(inventory_by_account):
    i_inventory = {}
    for account_inventory in inventory_by_account.values():
        for itype_and_az, count in account_inventory.items():
            if itype_and_az in i_inventory:
                i_inventory[itype_and_az] += count
            else:
                i_inventory[itype_and_az] = count
    return i_inventory


def get_ri_inventory(ri_account_credentials, region):
    access_key_id, secret_access_key = ri_account_credentials
    conn = boto.ec2.connect_to_region(region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    # first, find out which availability zones are present in the RI account
    supported_ri_zones = [] # just zone names
    zones = conn.get_all_zones()
    for z in zones:
        if z.state != 'available':
            raise RuntimeError("Zone %s state is not available, i.e. %s" % z.name, z.state)
        else:
            supported_ri_zones.append(z.name)

    # second, determine if there are still modifications that are being processed
    mod_filters = {'status' : 'processing'}
    processing_modifications = conn.describe_reserved_instances_modifications(filters=mod_filters)

    # and finally, compile the RI inventory for the RI account
    ri_inventory = {}

    ri_filters = {'state': 'active'} # possible RI Group states: active, retired, payment-pending, payment-failed
    for ri_group in conn.get_all_reserved_instances(filters=ri_filters):
        itype_and_az = ri_group.instance_type, ri_group.availability_zone
        if itype_and_az in ri_inventory:
            ri_inventory[itype_and_az] += ri_group.instance_count
        else:
            ri_inventory[itype_and_az] = ri_group.instance_count

    conn.close()
    return ri_inventory, supported_ri_zones, processing_modifications


def compute_ri_mistmatch(ri_inventory, i_inventory):
    mismatch = ri_inventory.copy()

    for itype_and_az, count in i_inventory.items():
        if itype_and_az not in mismatch:
            mismatch[itype_and_az] = 0
        mismatch[itype_and_az] -= count

    return {itype_and_az: diff for itype_and_az, diff in mismatch.items() if diff != 0}


def compute_ri_surplus(clean_mismatch):
    ri_surplus = {}
    # sum up all the on-demand/RI imbalances by instance type
    for (itype, az), diff in clean_mismatch.items():
        if itype not in ri_surplus:
            ri_surplus[itype] = 0
        ri_surplus[itype] += diff

    return ri_surplus


def greedy_distribution(mismatch):
    # separate into recepients and donors
    recepients = {}
    donors = {}
    for itype_and_az, diff in mismatch.items():
        if diff < 0:
            recepients[itype_and_az] = diff
        elif diff > 0:
            donors[itype_and_az] = diff

    plan = []

    for (recepient_itype, recepient_az), deficit in recepients.items():
        for donor_itype_and_az, count in donors.items():
            donor_itype, donor_az = donor_itype_and_az
            if donor_itype == recepient_itype:
                # greedily compensate the deficit
                move_count = min(abs(deficit), count)
                # update the plan with a new modification action
                plan.append((donor_itype, donor_az, recepient_az, move_count))
                # update the donor available count
                if count == move_count:
                    del donors[donor_itype_and_az]
                else:
                    donors[(donor_itype, donor_az)] -= move_count
                # update deficit
                deficit += move_count
                if deficit >= 0:
                    break

    return plan


def eliminate_unsupported_zones(mismatch, supported_ri_zones):
    # eliminate entries for zones that are not in a supported list.
    clean_mismatch = { itype_and_az : diff for itype_and_az, diff in mismatch.items() if itype_and_az[1] in supported_ri_zones }
    eliminated_i_inventory = { itype_and_az : -diff for itype_and_az, diff in mismatch.items() if itype_and_az[1] not in supported_ri_zones }
    return clean_mismatch, eliminated_i_inventory


def execute_plan(ri_account_credentials, region, plan, optimize):
    access_key_id, secret_access_key = ri_account_credentials
    conn = boto.ec2.connect_to_region(region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    ri_filters = {'state': 'active'}
    ri_groups = conn.get_all_reserved_instances(filters=ri_filters)

    modifications = {} # keyed by the source RI group

    for action in plan:
        itype, source_az, dest_az, count = action
        # necessary to check g.instance_count > 0 below because the following code could decrement it down to 0
        donor_groups = [g for g in ri_groups if g.instance_type == itype and g.availability_zone == source_az and g.instance_count > 0]
        index = 0
        while index < len(donor_groups) and count > 0:
            donor_group = donor_groups[index]
            move_count = min(count, donor_group.instance_count)
            if donor_group.id not in modifications:
                modifications[donor_group.id] = []
            move_descriptor = (donor_group, dest_az, move_count)
            modifications[donor_group.id].append(move_descriptor)
            count -= move_count
            donor_group.instance_count -= move_count
            index += 1

    modification_ids = []

    for modification in modifications.values():
        modification_ids.append(move_reserved_instances(conn, modification, optimize))

    conn.close()
    return modification_ids


def move_reserved_instances(conn, move_descriptor_list, optimize):
    assert len(move_descriptor_list) > 0
    donor_group_id = move_descriptor_list[0][0].id # id of the donor group in the first tuple
    target_configurations = []
    for donor_group, dest_az, move_count,  in move_descriptor_list:
        assert donor_group.id == donor_group_id # move_descriptor_list should contain one and the same RI group in all tuples
        config = boto.ec2.reservedinstance.ReservedInstancesConfiguration(availability_zone = dest_az, instance_count = move_count, platform = "EC2-VPC")
        target_configurations.append(config)
    if donor_group.instance_count > 0:
        target_configurations.append(boto.ec2.reservedinstance.ReservedInstancesConfiguration(availability_zone = donor_group.availability_zone, instance_count = donor_group.instance_count, platform = "EC2-VPC"))

    reserved_instance_ids = [donor_group_id]
    if optimize:
        return conn.modify_reserved_instances(client_token = str(time.time()), reserved_instance_ids = reserved_instance_ids, target_configurations = target_configurations)
    else:
        return 'rimod-<DRY-RUN>'


def publish_cw_metrics(ri_account_credentials, region, ri_surplus):
    access_key_id, secret_access_key = ri_account_credentials
    conn = boto.connect_cloudwatch(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    for itype, surplus in ri_surplus.items():
        conn.put_metric_data("RI-usage-%s" % region, "%s-available-RIs" % itype, surplus)

    conn.close()
