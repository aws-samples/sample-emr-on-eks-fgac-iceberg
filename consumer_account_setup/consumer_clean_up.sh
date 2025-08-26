# export AWS_REGION=us-west-2
# export PRODUCER_AWS_ACCOUNT=999333222111
# export EKSCLUSTER_NAME=fgac-blog

export ENVIRONMENT=dev
export CONSUMER_AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export PRODUCER_DATABASE=healthcare_db
export CONSUMER_DATABASE=consumer_healthcare_db
export rl_patients=rl_patients
export rl_claims=rl_claims
export patients=patients
export claims=claims
export S3_TEST_BUCKET=blog-emr-eks-fgac-test-$CONSUMER_AWS_ACCOUNT-$AWS_REGION-$ENVIRONMENT

export EMR_VC_NAME=emr-on-eks-$EKSCLUSTER_NAME
export TEAM1_JOB_ROLE_NAME=emr_on_eks_fgac_job_team1_execution_role
export TEAM2_JOB_ROLE_NAME=emr_on_eks_fgac_job_team2_execution_role
export QUERY_ROLE_NAME=emr_on_eks_fgac_query_execution_role

################################################################################################
#       Revoke permissions to Consumer account EMR on EKS Job Execution IAM role
################################################################################################

echo "============================================================================="
echo "  Revoke original table permissions to Consumer account ......"
echo "============================================================================="

for rbl in $patients $claims; do
  aws lakeformation revoke-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:role/${TEAM1_JOB_ROLE_NAME} \
    --permissions "SELECT" \
    --resource '{
        "Table": {
            "DatabaseName": "'${PRODUCER_DATABASE}'",
            "Name": "'${rbl}'",
            "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
        }
    }'
done

aws lakeformation revoke-permissions \
--principal DataLakePrincipalIdentifier=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:role/${TEAM2_JOB_ROLE_NAME} \
--permissions "SELECT" "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${PRODUCER_DATABASE}'",
        "Name": "'${claims}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

################################################################################################
#       Revoke resource link permissions to EMR on EKS Job Execution IAM role
################################################################################################

echo "============================================================================="
echo "  Revoke resource link table permissions to Consumer account ......"
echo "============================================================================="

for rl in $rl_patients $rl_claims; do
    aws lakeformation revoke-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:role/${TEAM1_JOB_ROLE_NAME} \
    --permissions "DESCRIBE" \
    --resource '{
        "Table": {
            "DatabaseName": "'${CONSUMER_DATABASE}'",
            "Name": "'${rl}'",
            "CatalogId": "'${CONSUMER_AWS_ACCOUNT}'"
        }
    }'
done    

aws lakeformation revoke-permissions \
--principal DataLakePrincipalIdentifier=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:role/${TEAM2_JOB_ROLE_NAME} \
--permissions "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${CONSUMER_DATABASE}'",
        "Name": "'${rl_claims}'",
        "CatalogId": "'${CONSUMER_AWS_ACCOUNT}'"
    }
}'

################################################################################################
#       Revoke Local Database permissions to EMR on EKS Job Execution IAM roles
################################################################################################

echo "============================================================================="
echo "  Revoke database permissions to EMR on EKS Job Execution IAM roles ......"
echo "============================================================================="
for role in $TEAM1_JOB_ROLE_NAME $TEAM2_JOB_ROLE_NAME; do
    aws lakeformation revoke-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:role/${role} \
    --permissions "DESCRIBE" \
    --resource '{
        "Database": {
            "Name": "'${CONSUMER_DATABASE}'"
        }
    }'
done

################################################################################################
#                      Delete Resource link to Glue table
################################################################################################

echo "============================================================================="
echo "  Drop Glue tables ......"
echo "============================================================================="
aws glue batch-delete-table --database-name $CONSUMER_DATABASE \
--tables-to-delete $rl_patients $rl_claims

##################################################################################################
###                  Delete local Database in Consumer account
##################################################################################################
echo "============================================================================="
echo "  Delete Cross-account HealthCare Database ......"
echo "============================================================================="
aws glue delete-database --name $CONSUMER_DATABASE

################################################################################################
#                  Delete IAM roles and policies
################################################################################################
echo "delete EMR on EKS IAM Job execution role for Team 1!"
export TEAM1_JOB_POLICY_ARN=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$TEAM1_JOB_ROLE_NAME-policy
echo "Detach policy"
aws iam detach-role-policy --role-name $TEAM1_JOB_ROLE_NAME --policy-arn $TEAM1_JOB_POLICY_ARN
echo "Delete IAM role"
aws iam delete-role --role-name $TEAM1_JOB_ROLE_NAME
echo "Delete Policy"
aws iam delete-policy --policy-arn $TEAM1_JOB_POLICY_ARN

echo "delete EMR on EKS IAM Job execution role for Team 2!"
export TEAM2_JOB_POLICY_ARN=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$TEAM2_JOB_ROLE_NAME-policy
echo "Detach policy"
aws iam detach-role-policy --role-name $TEAM2_JOB_ROLE_NAME --policy-arn $TEAM2_JOB_POLICY_ARN
echo "Delete IAM role"
aws iam delete-role --role-name $TEAM2_JOB_ROLE_NAME
echo "Delete Policy"
aws iam delete-policy --policy-arn $TEAM2_JOB_POLICY_ARN

echo "delete EMR on EKS IAM Query execution role!"
export QUERY_POLICY_ARN=arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$QUERY_ROLE_NAME-policy
echo "Detach policy"
aws iam detach-role-policy --role-name $QUERY_ROLE_NAME --policy-arn $QUERY_POLICY_ARN
echo "Delete IAM role"
aws iam delete-role --role-name $QUERY_ROLE_NAME
echo "Delete Policy"
aws iam delete-policy --policy-arn $QUERY_POLICY_ARN

################################################################################################
#                  Delete EMR on EKS Virtual cluster
################################################################################################

echo "============================================================================="
echo "                           delete EMR on EKS virtual cluster ......"
echo "============================================================================="
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '${EMR_VC_NAME}' && state == 'RUNNING'].id" --output text)
aws emr-containers delete-virtual-cluster --id $VIRTUAL_CLUSTER_ID

################################################################################################
#                  Delete EKS cluster
################################################################################################

echo "============================================================================="
echo "                           delete EKS cluster ......"
echo "============================================================================="

eksctl delete cluster --name $EKSCLUSTER_NAME

###################################################################################
#                  Delete S3 data and S3 bucket
###################################################################################
echo "============================================================================="
echo "  Delete S3 bucket data and delete S3 bucket ......"
echo "============================================================================="

aws s3 rm s3://$S3_TEST_BUCKET --recursive
aws s3api delete-bucket --bucket $S3_TEST_BUCKET