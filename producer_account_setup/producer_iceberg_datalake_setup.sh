# export AWS_REGION=us-west-2
# export PRODUCER_AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
# export CONSUMER_AWS_ACCOUNT=123456789012
# export ATHENA_RESULT_BUCKET=athena-results-$PRODUCER_AWS_ACCOUNT-$AWS_REGION

export ENVIRONMENT=dev
export DATABASE=healthcare_db
export patients=patients
export claims=claims
export S3_DATA_BUCKET=blog-emr-eks-fgac-data-$PRODUCER_AWS_ACCOUNT-$AWS_REGION-$ENVIRONMENT
export DATA_ACCESS_IAM_ROLE=lf_data_access_execution_role

####################################################################################
#            Create S3 bucket for iceberg table data
####################################################################################
echo "============================================================================="
echo "  Create S3 bucket for iceberg table data ......"
echo "============================================================================="

if [ $AWS_REGION -eq "us-east-1" ]
then
  echo "setup data bucket in us-east-1 region......"
  aws s3api create-bucket --bucket $S3_DATA_BUCKET --region $AWS_REGION
else
  echo "setup data bucket in another region......"
  aws s3api create-bucket --bucket $S3_DATA_BUCKET --region $AWS_REGION --create-bucket-configuration LocationConstraint=$AWS_REGION
fi

###################################################################################
#             Create DataAccess IAM Role for Lake formation
###################################################################################
echo "=========================================================="
echo "  Create DataAccess IAM Role for Lake formation ......"
echo "=========================================================="

cat >/tmp/FGAC_DataAccessRole_trust_policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "glue.amazonaws.com",
                    "lakeformation.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOL

cat >/tmp/FGAC_DataAccessRole_permission_policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::$S3_DATA_BUCKET"
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": [
                "s3:CreateObject",
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::$S3_DATA_BUCKET/warehouse",
                "arn:aws:s3:::$S3_DATA_BUCKET/warehouse/*"
            ]
        }
    ]
}
EOL

aws iam create-policy --policy-name $DATA_ACCESS_IAM_ROLE-policy --policy-document file:///tmp/FGAC_DataAccessRole_permission_policy.json
aws iam create-role --role-name $DATA_ACCESS_IAM_ROLE --assume-role-policy-document file:///tmp/FGAC_DataAccessRole_trust_policy.json
aws iam attach-role-policy --role-name $DATA_ACCESS_IAM_ROLE --policy-arn arn:aws:iam::$PRODUCER_AWS_ACCOUNT:policy/$DATA_ACCESS_IAM_ROLE-policy


##############################################################################################
#            Register S3 bucket as data location with DataAccess IAM Role for Lake formation
##############################################################################################
echo "============================================================================================="
echo "  Register S3 bucket as data location with DataAccess IAM Role for Lake formation ......"
echo "============================================================================================="
aws lakeformation register-resource \
 --resource-arn arn:aws:s3:::$S3_DATA_BUCKET/warehouse \
 --role-arn arn:aws:iam::$PRODUCER_AWS_ACCOUNT:role/$DATA_ACCESS_IAM_ROLE

###################################################################################
#                  Create Glue Database: healthcare_db
###################################################################################
echo "============================================================================="
echo "  Create Glue Database ......"
echo "============================================================================="
aws glue create-database \
    --database-input "{\"Name\":\"$DATABASE\", \"LocationUri\": \"s3://"$S3_DATA_BUCKET"/warehouse/\"}"

##################################################################################
#                                Create Patients Glue table
##################################################################################
echo "============================================================================="
echo "  Create Patients Glue table ......"
echo "============================================================================="
aws athena start-query-execution \
--query-string "CREATE TABLE $DATABASE.$patients (
    patient_id BIGINT,
    patient_name STRING,
    date_of_birth DATE,
    gender STRING,
    city STRING,
    state STRING,
    ssn STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (city)
LOCATION 's3://$S3_DATA_BUCKET/warehouse/$DATABASE/$patients/'
TBLPROPERTIES (
    'table_type'='ICEBERG'
);" \
--query-execution-context "Database=$DATABASE" \
--result-configuration "OutputLocation=s3://$ATHENA_RESULT_BUCKET/"

###################################################################################
#       Insert records into Patients glue table using Athena query
###################################################################################
echo "============================================================================="
echo "  Insert records into Patients glue table using Athena query ......"
echo "============================================================================="

aws athena start-query-execution \
--query-string "INSERT INTO $DATABASE.$patients
VALUES
    (1001, 'fgac1 John', DATE '1980-05-15', 'M', 'Los Angeles', 'California', '123-45-6789', TIMESTAMP '2025-03-28 10:00:00', TIMESTAMP '2025-03-28 10:00:00'),
    (1002, 'fgac2 Maria', DATE '1992-08-22', 'F', 'San Francisco', 'California', '234-56-7890', TIMESTAMP '2025-03-28 10:05:00', TIMESTAMP '2025-03-28 10:05:00'),
    (1003, 'fgac3 David', DATE '1975-12-01', 'M', 'San Diego', 'California', '345-67-8901', TIMESTAMP '2025-03-28 10:10:00', TIMESTAMP '2025-03-28 10:10:00'),
    (1004, 'fgac4 Sarah', DATE '1988-03-30', 'F', 'Sacramento', 'California', '456-78-9012', TIMESTAMP '2025-03-28 10:15:00', TIMESTAMP '2025-03-28 10:15:00'),
    (1005, 'fgac5 Robert', DATE '1995-07-07', 'M', 'Houston', 'Texas', '567-89-0123', TIMESTAMP '2025-03-28 10:20:00', TIMESTAMP '2025-03-28 10:20:00'),
    (1006, 'fgac6 Emily', DATE '1982-11-18', 'F', 'Austin', 'Texas', '678-90-1234', TIMESTAMP '2025-03-28 10:25:00', TIMESTAMP '2025-03-28 10:25:00'),
    (1007, 'fgac7 Michael', DATE '1979-09-25', 'M', 'Dallas', 'Texas', '789-01-2345', TIMESTAMP '2025-03-28 10:30:00', TIMESTAMP '2025-03-28 10:30:00'),
    (1008, 'fgac8 Lisa', DATE '1990-02-14', 'F', 'San Antonio', 'Texas', '890-12-3456', TIMESTAMP '2025-03-28 10:35:00', TIMESTAMP '2025-03-28 10:35:00'),
    (1009, 'fgac9 James', DATE '1987-06-03', 'M', 'New York City', 'New York', '901-23-4567', TIMESTAMP '2025-03-28 10:40:00', TIMESTAMP '2025-03-28 10:40:00'),
    (1010, 'fgac10 Amanda', DATE '1993-04-11', 'F', 'Buffalo', 'New York', '012-34-5678', TIMESTAMP '2025-03-28 10:45:00', TIMESTAMP '2025-03-28 10:45:00'),
    (1011, 'fgac11 Kevin', DATE '1985-08-19', 'M', 'Rochester', 'New York', '123-45-6789', TIMESTAMP '2025-03-28 10:50:00', TIMESTAMP '2025-03-28 10:50:00'),
    (1012, 'fgac12 Rachel', DATE '1991-12-25', 'F', 'Albany', 'New York', '234-56-7890', TIMESTAMP '2025-03-28 10:55:00', TIMESTAMP '2025-03-28 10:55:00')
" \
--query-execution-context "Database=$DATABASE" \
--result-configuration "OutputLocation=s3://$ATHENA_RESULT_BUCKET/"

###################################################################################
#                                Create Claims Glue table
###################################################################################
echo "============================================================================="
echo "  Create Claims Glue table ......"
echo "============================================================================="

aws athena start-query-execution \
--query-string "CREATE TABLE $DATABASE.$claims (
    claim_id STRING,
    patient_id BIGINT,
    claim_date DATE,
    diagnosis_code STRING,
    procedure_code STRING,
    amount DECIMAL(10,2),
    status STRING,
    provider_id STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (status)
LOCATION 's3://$S3_DATA_BUCKET/warehouse/$DATABASE/$claims/'
TBLPROPERTIES (
    'table_type'='ICEBERG'
);" \
--query-execution-context "Database=$DATABASE" \
--result-configuration "OutputLocation=s3://$ATHENA_RESULT_BUCKET/"

###################################################################################
#       Insert records into Claims glue table using Athena query
###################################################################################
echo "============================================================================="
echo "  Insert records into Claims glue table using Athena query ......"
echo "============================================================================="

aws athena start-query-execution \
--query-string "INSERT INTO $DATABASE.$claims
VALUES
    ('CLM001', 1001, DATE '2025-03-15', 'J45.901', '99213', 150.00, 'Approved', 'DR123', TIMESTAMP '2025-03-28 11:00:00', TIMESTAMP '2025-03-28 11:00:00'),
    ('CLM002', 1002, DATE '2025-03-20', 'M54.5', '97110', 200.00, 'Pending', 'DR456', TIMESTAMP '2025-03-28 11:05:00', TIMESTAMP '2025-03-28 11:05:00'),
    ('CLM003', 1003, DATE '2025-03-25', 'I10', '99214', 175.00, 'Approved', 'DR789', TIMESTAMP '2025-03-28 11:10:00', TIMESTAMP '2025-03-28 11:10:00'),
    ('CLM004', 1004, DATE '2025-03-18', 'E11.9', '82947', 80.00, 'Approved', 'DR234', TIMESTAMP '2025-03-28 11:15:00', TIMESTAMP '2025-03-28 11:15:00'),
    ('CLM005', 1005, DATE '2025-03-22', 'J30.1', '95004', 300.00, 'Pending', 'DR567', TIMESTAMP '2025-03-28 11:20:00', TIMESTAMP '2025-03-28 11:20:00'),
    ('CLM006', 1006, DATE '2025-03-27', 'K21.9', '43235', 500.00, 'Approved', 'DR890', TIMESTAMP '2025-03-28 11:25:00', TIMESTAMP '2025-03-28 11:25:00'),
    ('CLM007', 1007, DATE '2025-03-16', 'M25.511', '73560', 125.00, 'Denied', 'DR012', TIMESTAMP '2025-03-28 11:30:00', TIMESTAMP '2025-03-28 11:30:00'),
    ('CLM008', 1008, DATE '2025-03-21', 'N39.0', '81001', 50.00, 'Approved', 'DR345', TIMESTAMP '2025-03-28 11:35:00', TIMESTAMP '2025-03-28 11:35:00'),
    ('CLM009', 1009, DATE '2025-03-26', 'L40.0', '96910', 250.00, 'Pending', 'DR678', TIMESTAMP '2025-03-28 11:40:00', TIMESTAMP '2025-03-28 11:40:00'),
    ('CLM010', 1010, DATE '2025-03-19', 'F41.1', '90834', 180.00, 'Approved', 'DR901', TIMESTAMP '2025-03-28 11:45:00', TIMESTAMP '2025-03-28 11:45:00')
" \
--query-execution-context "Database=$DATABASE" \
--result-configuration "OutputLocation=s3://$ATHENA_RESULT_BUCKET/"


###################################################################################
 #      Grant database permissions to Consumer account
###################################################################################
echo "============================================================================="
echo "  Grant database permissions to Consumer account ......"
echo "============================================================================="

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions DESCRIBE \
--resource '{
    "Database": {
        "Name": "'${DATABASE}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'


###################################################################################
#       Create Column level filter on Patients table to Consumer account
###################################################################################
echo "============================================================================="
echo "  Create Column level filter on Patients table to Consumer account ......"
echo "============================================================================="
# Include all columns except "ssn"

cat >/tmp/patients_data_filter.json <<EOL
{
    "TableData": {
        "ColumnNames": ["patient_id", "patient_name", "date_of_birth", "gender", "city", "state", "created_at", "updated_at"],
        "DatabaseName":"${DATABASE}",
        "Name": "patients_column_row_filter",
        "RowFilter": {
            "FilterExpression": "state in ('Texas', 'New York')"
        },
        "TableCatalogId": "${PRODUCER_AWS_ACCOUNT}",
        "TableName": "${patients}"
    }
}
EOL

aws lakeformation create-data-cells-filter \
--cli-input-json file:///tmp/patients_data_filter.json

###################################################################################
#       Grant Database permissions to Consumer account
###################################################################################
echo "============================================================================="
echo "  Grant Database permissions to Consumer account ......"
echo "============================================================================="

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "DESCRIBE" \
--permissions-with-grant-option "DESCRIBE" \
--resource '{
    "Database": {
        "Name": "'${DATABASE}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

###################################################################################
#       Grant Column level filter on Patients table to Consumer account
###################################################################################
echo "============================================================================="
echo "  Grant Column level filter on Patients table to Consumer account ......"
echo "============================================================================="

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "SELECT" \
--permissions-with-grant-option "SELECT" \
--resource '{
    "DataCellsFilter": {
        "TableCatalogId" : "'${PRODUCER_AWS_ACCOUNT}'",
        "DatabaseName": "'${DATABASE}'",
        "TableName": "'${patients}'",
        "Name": "patients_column_row_filter"
    }
}'

###################################################################################
#       Grant Claims table permissions to Consumer account
###################################################################################
echo "============================================================================="
echo "  Grant Claims table permissions to Consumer account ......"
echo "============================================================================="

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier=${CONSUMER_AWS_ACCOUNT} \
--permissions "SELECT" "DESCRIBE" \
--permissions-with-grant-option "SELECT" "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${DATABASE}'",
        "Name": "'${claims}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'