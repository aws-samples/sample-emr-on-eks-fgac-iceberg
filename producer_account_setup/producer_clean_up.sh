export AWS_REGION=us-west-2
export PRODUCER_AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CONSUMER_AWS_ACCOUNT=123456789012
export ENVIRONMENT=dev

export database=healthcare_db
export patients=patients
export claims=claims
export S3_DATA_BUCKET=blog-emr-eks-fgac-data-$PRODUCER_AWS_ACCOUNT-$AWS_REGION-$ENVIRONMENT
export DATA_ACCESS_IAM_ROLE=lf_data_access_execution_role

##################################################################################
#      Revoke table permissions to Consumer account
##################################################################################
echo "============================================================================="
echo "  Revoke table permissions to Consumer account ......"
echo "============================================================================="
aws lakeformation revoke-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "SELECT" "DESCRIBE" \
--permissions-with-grant-option "SELECT" "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${database}'",
        "Name": "'${claims}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

##################################################################################
#      Revoke data-cell level filtering permissions to Consumer account
##################################################################################
echo "============================================================================="
echo "  Revoke data-cell level filtering permissions to Consumer account ......"
echo "============================================================================="
aws lakeformation revoke-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "SELECT" \
--permissions-with-grant-option "SELECT" \
--resource '{
    "DataCellsFilter": {
        "TableCatalogId" : "'${PRODUCER_AWS_ACCOUNT}'",
        "DatabaseName": "'${database}'",
        "TableName": "'${patients}'",
        "Name": "patients_column_row_filter"
    }
}'

##################################################################################
#      Delete data-cell level filter from Producer account
##################################################################################
echo "============================================================================="
echo "  Delete data-cell level filter from Producer account ......"
echo "============================================================================="
aws lakeformation delete-data-cells-filter \
--table-catalog-id=${PRODUCER_AWS_ACCOUNT} \
--database-name=${database} \
--table-name="'${patients}'" \
--name="patients_column_row_filter"

###################################################################################
#       Revoke database permissions to Consumer account
###################################################################################
echo "============================================================================="
echo "  Revoke database permissions to Consumer account ......"
echo "============================================================================="
aws lakeformation revoke-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "DESCRIBE" \
--resource '{
    "Database": {
        "Name": "'${database}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

###################################################################################
#       Deregister S3 bucket as data location from Producer account Lake Formation
###################################################################################
echo "============================================================================="
echo "  Deregister S3 bucket as data location from Lake Formation ......"
echo "============================================================================="
aws lakeformation  deregister-resource \
--resource-arn "arn:aws:s3:::$S3_DATA_BUCKET/warehouse"

###################################################################################
#                            Drop Glue tables
###################################################################################
echo "============================================================================="
echo "  Drop Glue tables ......"
echo "============================================================================="
aws glue batch-delete-table \
--database-name $database \
--tables-to-delete $table1 $table2

###################################################################################
#                            Delete HealthCare Database
###################################################################################
echo "============================================================================="
echo "  Delete HealthCare Database ......"
echo "============================================================================="
aws glue delete-database \
--name $database

###################################################################################
#                            Delete S3 data and S3 bucket
###################################################################################
echo "============================================================================="
echo "  Delete S3 bucket data and delete S3 bucket ......"
echo "============================================================================="
echo "delete S3 bucket $S3_DATA_BUCKET"
aws s3 rm s3://$S3_DATA_BUCKET --recursive
aws s3api delete-bucket --bucket $S3_DATA_BUCKET

###################################################################################
#                            Delete IAM Policies and IAM Role
###################################################################################
echo "============================================================================="
echo "  Delete IAM Policies and IAM Role ......"
echo "============================================================================="
export DATA_ACCESS_POLICY_ARN=arn:aws:iam::$PRODUCER_AWS_ACCOUNT:policy/$DATA_ACCESS_IAM_ROLE-policy
echo "Detach policy"
aws iam detach-role-policy --role-name $DATA_ACCESS_IAM_ROLE --policy-arn $DATA_ACCESS_POLICY_ARN
echo "Delete IAM role"
aws iam delete-role --role-name $DATA_ACCESS_IAM_ROLE
echo "Delete Policy"
aws iam delete-policy --policy-arn $DATA_ACCESS_POLICY_ARN