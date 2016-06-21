"""
This is the main example script to execute, it is meant as an example
of how the riptimize.py module is to be used, and effectivey acts as
the driver of the module with rudimentary console UI + CSV report
generation and S3 upload. It's job is to demonstrate the functionality
of riptimize and it is not meant to execute in production as is.

The step-by-step instructions as to how to execute this script is
embedded in comments below labeled with STEP X OF X.
"""
import riptimize
import datetime
import csv
import boto

def main():
    print "Example Riptimize Driver"
    print

    # 1. setup
    # STEP 1 of 7: specify region
    region = 'us-east-1'
    # STEP 2 of 7: set the RI holding account id and credentials
    ri_account_id = 'RIRI-RIRI-RIRI' # replace with actual AWS Account ID
    ri_account_credentials = ('<access-key-id-ri>', '<secret_access-key-ri>')

    all_accounts = {ri_account_id: ri_account_credentials}
    # STEP 3 of 7: add ids and credentials for all other linked accounts, at first just add a couple other accounts
#    all_accounts['AAAA-AAAA-AAAA'] = ('<access-key-id-a>', '<secret-access-key-a>')
#    all_accounts['BBBB-BBBB-BBBB'] = ('<access-key-id-b>', '<secret-access-key-b>')
#    ...
#    all_accounts['ZZZZ-ZZZZ-ZZZZ'] = ('<access-key-id-z>', '<secret-access-key-z>')

    # STEP 4 of 7: For the first few tests this should be set to False
    # once you see that the script is running, change to True to actually execute RI modifications
    optimize = False # if False, means a DRY-RUN

    # STEP 5 of 7: Leaving as True will publish RI surplus metrics to CloudWatch
    publish_metrics = True # custom metrics are created in AWS CloudWatch

    # STEP 6 of 7: Leaving as True will upload the CSV report to S3 for safekeeping
    upload_report = True # CSV reports will be saved in S3 in s3_report_bucket
    s3_report_bucket = "riptimize-reports-%s" % ri_account_id

    # 2. do it
    # STEP 7 of 7: Ok, you are ready to go, just execute on the command line % python example_main.py 
    riptimize_result_tuple = riptimize.riptimize(all_accounts, ri_account_credentials, region, optimize, publish_metrics)

    # 3. show results
    i_inventory, i_inventory_by_account, ri_inventory, supported_ri_zones, processing_modifications, clean_mismatch, recommendations, plan, modification_ids = riptimize_result_tuple

    time_now = datetime.datetime.utcnow()
    print "Report for region %s as of %s" % (region, time_now)
    print
    # 3.1 print on-demand instance inventory
    print "Instance Inventory by account:"
    print i_inventory_by_account
    print
    print "Aggregate instance inventory:"
    print i_inventory
    print
    # 3.2 print RI inventory
    print "RI Inventory:"
    print ri_inventory
    print
    # 3.3 show all supported AZs in the RI holding account
    print "Supported RI zones: " + str(supported_ri_zones)
    # 3.4 show if previous modifications are still being executed
    modifications_inflight = len(processing_modifications) != 0
    if modifications_inflight:
        print
        print "======--- WARNING ---======"
        print "Previous modifications are still processing:"
        for mod in processing_modifications:
            print "modification_id: %s, status: %s" % (mod.modification_id, mod.status)
        print "!!! RI optimizations cannot be performed until previous modifications are completed"
        print "!!! RI inventory and recommendations will also be potentially incorrect"
    print
    # 3.5 print detected mismatches between numbers of on-demand running instances and RIs by availability zone and instance type
    if len(clean_mismatch) > 0:
        print "On-demand/RI inventory mismatches per availability zone:"
        print clean_mismatch
    else:
        print "No On-demand/RI inventory mimatches detected in any availability zones:"
    print
    # 3.6 print recommendations for migrating running instances into AZs covered by RI holding account, purchasing additional RIs or launching additional instances to get better RI utilization
    eliminated_i_inventory, ri_imbalance = recommendations
    if len(eliminated_i_inventory) == 0 and len(ri_imbalance) == 0:
        print "No recomendations available"
    else:
        print "Recommendations:"
        if len(eliminated_i_inventory) > 0:
            print "\tOn-demand instances running in zones not supported by RIs. Migrate them to supported zones:"
            print "\t" + str(eliminated_i_inventory)
        print
        if len(ri_imbalance) > 0:
            print "\tOn-demand/RI imbalance detected!"
            print "\tNegative numbers indicate additional RIs needed, positive ones indicate that RIs are underutilized and more instances can be launched:"
            print "\t" + str(ri_imbalance)
    print
    # 3.7 print high-level optimization plan if one is possible, showing how many RIs need to be moved to which AZs
    if len(plan) == 0:
        print "No RI redistribution is possible."
    else:
        print "RI Optimization possible! Plan: " + str(plan)
        if optimize:
            if modifications_inflight:
                print "Previous optimizations are still processing, new optimizations kicked off in DRY-RUN mode only!"
            else:
                print "Optimize option selected, optimizations kicked-off..."
        else:
            print "Optimize flag not set, so optimizations kicked off in DRY-RUN mode only!"

        print
        # 3.8 finally, if optimizations were actually kicked off, list all modification ids, or fake ones in case of a dry run
        print "Initiated optimizations:"
        print modification_ids

    filename_safe_timestamp = str(time_now).replace(' ','_').replace(':', '-')
    report_file_name = "riptimize_report_%s_%s.csv" % (region, filename_safe_timestamp)

    csv_report(report_file_name, time_now, region, i_inventory_by_account, ri_inventory, clean_mismatch, plan, modification_ids)
    print
    print "CSV report written to %s" % report_file_name

    if upload_report:
        upload_report_to_s3(ri_account_credentials, report_file_name, s3_report_bucket)
        print
        print "Report uploaded to S3 as %s/%s of RI holding account %s" % (s3_report_bucket, report_file_name, ri_account_id)

    print
    print "Done"

# exapmle of generating a CSV report
def csv_report(csv_file_name, time_now, region, i_inventory_by_account, ri_inventory, clean_mismatch, plan, modification_ids):
    with open(csv_file_name, 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Report for region %s at %s" % (region, str(time_now))])
        # write instance inventory report
        writer.writerow([])
        writer.writerow(['Instance Inventory'])
        writer.writerow(['Account ID', 'Instance Type', 'Availability Zone', 'Count'])
        for account_id, inventory_for_account in i_inventory_by_account.items():
            for (itype, az), count in inventory_for_account.items():
                writer.writerow([account_id, itype, az, count])
        # write RI inventory report
        writer.writerow([])
        writer.writerow(['RI Inventory'])
        writer.writerow(['Instance Type', 'Availability Zone', 'Count'])
        for (itype, az), count in ri_inventory.items():
            writer.writerow([itype, az, count])
        # write report on On-demand/RI inventory mismatches
        writer.writerow([])
        writer.writerow(['On-demand/RI inventory mismatches per each availability zone'])
        writer.writerow(['Instance Type', 'Availability Zone', 'Diff'])
        for (itype, az), count in clean_mismatch.items():
            writer.writerow([itype, az, count])
        # write optimization plan
        writer.writerow([])
        writer.writerow(['RI modification plan'])
        writer.writerow(['Instance Type', 'Source AZ', 'Destination AZ', 'Count'])
        for itype, source_az, dest_az, count in plan:
            writer.writerow([itype, source_az, dest_az, count])
        # write modification_ids
        writer.writerow([])
        writer.writerow(['Kicked off RI modifications'])
        writer.writerow(['Modification ID'])
        for modification_id in modification_ids:
            writer.writerow([modification_id])


def upload_report_to_s3(ri_account_credentials, report_file_name, s3_report_bucket):
    access_key_id, secret_access_key = ri_account_credentials
    s3 = boto.connect_s3(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    # create bucket if does not exist
    bucket = s3.lookup(s3_report_bucket)
    if not bucket:
        bucket = s3.create_bucket(s3_report_bucket)

    # upload the report
    key = bucket.new_key(report_file_name)
    key.set_contents_from_filename(report_file_name)

    s3.close()

if __name__ == '__main__':
    main()
