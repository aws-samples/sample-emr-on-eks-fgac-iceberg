export AWS_REGION=us-west-2
export PRODUCER_AWS_ACCOUNT=999333222111
export CONSUMER_AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export ENVIRONMENT=dev

export PRODUCER_DATABASE=healthcare_db
export CONSUMER_DATABASE=consumer_healthcare_db
export rl_patients=rl_patients
export rl_claims=rl_claims
export patients=patients
export claims=claims
export S3_TEST_BUCKET=blog-emr-eks-fgac-test-$CONSUMER_AWS_ACCOUNT-$AWS_REGION-$ENVIRONMENT
export S3_DATA_BUCKET=blog-emr-eks-fgac-data-$PRODUCER_AWS_ACCOUNT-$AWS_REGION-$ENVIRONMENT

export TEAM1_JOB_ROLE_NAME=emr_on_eks_fgac_job_team1_execution_role
export TEAM2_JOB_ROLE_NAME=emr_on_eks_fgac_job_team2_execution_role
export QUERY_ROLE_NAME=emr_on_eks_fgac_query_execution_role
export EKSCLUSTER_NAME=emr-on-eks-fgac-blog
export EMR_LF_USER_NAMESPACE=lf-fgac-blog-user
export EMR_LF_SECURE_NAMESPACE=lf-fgac-blog-secure
export EKS_VERSION=1.3
export SECURITY_CONFIG_NAME=emr-on-eks-fgac-testing-blog
export EMR_VC_NAME=emr-on-eks-vc
export EMR_ON_EKS_TEAM1_JOB_NAME=patients_care_team_query_tables
export EMR_ON_EKS_TEAM2_JOB_NAME=claims_care_team_query_tables
###################################################################################
#                  Create local Database in Consumer account
###################################################################################
echo "==========================================================="
echo "  Create local database......"
echo "==========================================================="

aws glue create-database \
    --database-input "{\"Name\":\"$CONSUMER_DATABASE\"}"

###################################################################################
#                  create S3 bucket for EKS job script and spark logs
###################################################################################

# create S3 bucket for application
if [ $AWS_REGION -eq "us-east-1" ]
then
  echo "setup data bucket in us-east-1 region......"
  aws s3api create-bucket --bucket $S3_TEST_BUCKET --region $AWS_REGION
else
  echo "setup data bucket in another region......"
  aws s3api create-bucket --bucket $S3_TEST_BUCKET --region $AWS_REGION --create-bucket-configuration LocationConstraint=$AWS_REGION
fi

###################################################################################
#                      Create Resource link to Glue table
###################################################################################

echo "======================================================================="
echo "  Create resource link to shared patients table in local database......"
echo "======================================================================="


aws glue create-table \
--database-name $CONSUMER_DATABASE \
--table-input '
  {
    "Name":"'$rl_patients'",
    "TargetTable":{
      "CatalogId":"'$PRODUCER_AWS_ACCOUNT'",
      "DatabaseName":"'${PRODUCER_DATABASE}'",
      "Name":"'$patients'"
    }
  }'

###################################################################################
#                      Create Resource link to Glue table
###################################################################################

echo "====================================================================="
echo "  Create resource link to shared claims table in local database......"
echo "====================================================================="

aws glue create-table \
--database-name $CONSUMER_DATABASE \
--table-input '
  {
    "Name":"'$rl_claims'",
    "TargetTable":{
      "CatalogId":"'$PRODUCER_AWS_ACCOUNT'",
      "DatabaseName":"'${PRODUCER_DATABASE}'",
      "Name":"'$claims'"
    }
  }'


##############################################################################################################
#      Create query execution iam role
###############################################################################################################

echo "==================================================="
echo "  Create EMR on EKS Query Execution IAM Role ......"
echo "==================================================="

cat >/tmp/query-execution-policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EmrGetCertificate",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
                {
            "Sid": "AssumeJobExecutionRole",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Resource": ["arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME", "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME"],
            "Condition": {
                "StringLike": {
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "EMR on EKS Engine"
                }
            }
        },
        {
            "Sid": "AssumeJobExecutionRoleOnly",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Resource": ["arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME", "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME"]
        }
    ]
}
EOL

cat >/tmp/query-trust-policy.json <<EOL
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
          "AWS": "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {}
    }]
}
EOL

aws iam create-policy --policy-name $QUERY_ROLE_NAME-policy --policy-document file:///tmp/query-execution-policy.json
aws iam create-role --role-name $QUERY_ROLE_NAME --assume-role-policy-document file:///tmp/query-trust-policy.json
aws iam attach-role-policy --role-name $QUERY_ROLE_NAME --policy-arn arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$QUERY_ROLE_NAME-policy
sleep 5

##############################################################################################################
#      Create a job execution role (https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/creating-job-execution-role.html)
###############################################################################################################

echo "==========================================================="
echo "  Create EMR on EKS Job Execution IAM Role for Team 1......"
echo "==========================================================="

cat >/tmp/team1-job-execution-policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EmrGetCertificate",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
        {
            "Sid": "LakeFormationManagedAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "glue:Get*",
                "glue:Create*",
                "glue:Update*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "EmrSparkJobAccess",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
              "arn:aws:s3:::${S3_TEST_BUCKET}",
              "arn:aws:s3:::${S3_TEST_BUCKET}/*"
            ]
        },
        {
            "Sid": "ListBucketAccessForLogSync",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
              "arn:aws:s3:::${S3_TEST_BUCKET}",
              "arn:aws:s3:::${S3_TEST_BUCKET}/*"
            ]
        }
    ]
}
EOL

cat >/tmp/team1-job-trust-policy.json <<EOL
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Sid": "TrustQueryEngineRoleToAssume",
        "Effect": "Allow",
        "Principal": {
            "AWS": "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$QUERY_ROLE_NAME"
        },
        "Action": [
            "sts:AssumeRole",
            "sts:TagSession"
        ],
        "Condition": {
            "StringLike": {
                "aws:RequestTag/LakeFormationAuthorizedCaller": "EMR on EKS Engine"
            }
        }
    },
    {
        "Sid": "TrustQueryEngineRoleToAssumeRoleOnly",
        "Effect": "Allow",
        "Principal": {
            "AWS": "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$QUERY_ROLE_NAME"
        },
        "Action": "sts:AssumeRole"
    } ]
}
EOL

aws iam create-policy --policy-name $TEAM1_JOB_ROLE_NAME-policy --policy-document file:///tmp/team1-job-execution-policy.json
aws iam create-role --role-name $TEAM1_JOB_ROLE_NAME --assume-role-policy-document file:///tmp/team1-job-trust-policy.json
aws iam attach-role-policy --role-name $TEAM1_JOB_ROLE_NAME --policy-arn arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$TEAM1_JOB_ROLE_NAME-policy

echo "==========================================================="
echo "  Create EMR on EKS Job Execution IAM Role fro Team 2......"
echo "==========================================================="

cat >/tmp/team2-job-execution-policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EmrGetCertificate",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
        {
            "Sid": "LakeFormationManagedAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "glue:Get*",
                "glue:Create*",
                "glue:Update*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "EmrSparkJobAccess",
            "Effect": "Allow",
            "Action": [
                "s3:CreateObject",
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
              "arn:aws:s3:::${S3_TEST_BUCKET}",
              "arn:aws:s3:::${S3_TEST_BUCKET}/*"
            ]
        },
        {
            "Sid": "ListBucketAccessForLogSync",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
              "arn:aws:s3:::${S3_TEST_BUCKET}",
              "arn:aws:s3:::${S3_TEST_BUCKET}/*"
            ]
        }
    ]
}
EOL

cat >/tmp/team2-job-trust-policy.json <<EOL
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Sid": "TrustQueryEngineRoleToAssume",
        "Effect": "Allow",
        "Principal": {
            "AWS": "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$QUERY_ROLE_NAME"
        },
        "Action": [
            "sts:AssumeRole",
            "sts:TagSession"
        ],
        "Condition": {
            "StringLike": {
                "aws:RequestTag/LakeFormationAuthorizedCaller": "EMR on EKS Engine"
            }
        }
    },
    {
        "Sid": "TrustQueryEngineRoleToAssumeRoleOnly",
        "Effect": "Allow",
        "Principal": {
            "AWS": "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$QUERY_ROLE_NAME"
        },
        "Action": "sts:AssumeRole"
    } ]
}
EOL

aws iam create-policy --policy-name $TEAM2_JOB_ROLE_NAME-policy --policy-document file:///tmp/team2-job-execution-policy.json
aws iam create-role --role-name $TEAM2_JOB_ROLE_NAME --assume-role-policy-document file:///tmp/team2-job-trust-policy.json
aws iam attach-role-policy --role-name $TEAM2_JOB_ROLE_NAME --policy-arn arn:aws:iam::$CONSUMER_AWS_ACCOUNT:policy/$TEAM2_JOB_ROLE_NAME-policy

echo "==============================================="
echo "  Create EKS Cluster ......"
echo "==============================================="
echo $EKSCLUSTER_NAME

eksctl create cluster --name $EKSCLUSTER_NAME --region $AWS_REGION

#This step creates an IAM OIDC provider for the EKS cluster that you just created and can enable to use AWS Identity and Access Management (IAM) roles for service accounts. Details: https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html.
eksctl utils associate-iam-oidc-provider --cluster $EKSCLUSTER_NAME --approve

## if EKS cluster exists, comment out the line above, uncomment this line
## eksctl create nodegroup -f /tmp/ekscluster.yaml
aws eks update-kubeconfig --name $EKSCLUSTER_NAME --region $AWS_REGION

echo "==============================================="
echo "  Enable EMR on EKS ......"
echo "==============================================="

# Create kubernetes namespace for EMR on EKS for process isolation in EKS.
kubectl create namespace $EMR_LF_USER_NAMESPACE
kubectl create namespace $EMR_LF_SECURE_NAMESPACE
kubectl get namespace

# RBAC Permissions
# Namespaces roles and rolebinding

cat <<EOF >/tmp/emr-containers-role.yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: emr-containers
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["serviceaccounts", "services", "configmaps", "events", "pods", "pods/log"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "patch", "delete", "watch"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: ["extensions", "networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["roles", "rolebindings"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
EOF

cat <<EOF >/tmp/emr-containers-rb.yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: emr-containers
subjects:
  - kind: User
    name: emr-containers
    apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f /tmp/emr-containers-role.yaml -n $EMR_LF_USER_NAMESPACE
kubectl apply -f /tmp/emr-containers-role.yaml -n $EMR_LF_SECURE_NAMESPACE

kubectl apply -f /tmp/emr-containers-rb.yaml -n $EMR_LF_USER_NAMESPACE
kubectl apply -f /tmp/emr-containers-rb.yaml -n $EMR_LF_SECURE_NAMESPACE

#Cluster Role and Role Binding

cat <<EOF >/tmp/emr-containers-cluster-role.yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: emr-containers
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["serviceaccounts", "services", "configmaps", "events", "pods", "pods/log"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "patch", "delete", "watch"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["extensions", "networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["clusterroles","clusterrolebindings","roles", "rolebindings"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete",  "deletecollection", "annotate", "patch", "label"]
EOF

cat <<EOF >/tmp/emr-containers-cluster-role-binding.yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: emr-containers
subjects:
- kind: User
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f /tmp/emr-containers-cluster-role.yaml
kubectl apply -f /tmp/emr-containers-cluster-role-binding.yaml

echo "==============================================="
echo "  Update EKS cluster nodegroup ......"
echo    $EKSCLUSTER_NAME
echo "==============================================="
export eks_node_group_id=$(aws eks list-nodegroups \
    --cluster-name $EKSCLUSTER_NAME \
    --query 'nodegroups[0]' --output text)
echo $eks_node_group_id

eksctl scale nodegroup --name=$eks_node_group_id --cluster=$EKSCLUSTER_NAME --nodes=10 --nodes-min=10 --nodes-max=20

echo "==============================================="
echo "  Update IAM role Trust policies ......"
echo    $EKSCLUSTER_NAME
echo "==============================================="
# Add eks cluster trust policy
aws emr-containers update-role-trust-policy --cluster-name $EKSCLUSTER_NAME --namespace $EMR_LF_USER_NAMESPACE --role-name $TEAM2_JOB_ROLE_NAME
aws emr-containers update-role-trust-policy --cluster-name $EKSCLUSTER_NAME --namespace $EMR_LF_USER_NAMESPACE --role-name $TEAM1_JOB_ROLE_NAME
aws emr-containers update-role-trust-policy --cluster-name $EKSCLUSTER_NAME --namespace $EMR_LF_SECURE_NAMESPACE --role-name $QUERY_ROLE_NAME

###################################################################################
 #      Grant database permissions to EMR on EKS Job Execution IAM role
###################################################################################
echo "Grant permission on local database"

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME"  \
--permissions DESCRIBE \
--resource '{
    "Database": {
        "Name": "'${CONSUMER_DATABASE}'"
    }
}'

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME"  \
--permissions DESCRIBE \
--resource '{
    "Database": {
        "Name": "'${CONSUMER_DATABASE}'"
    }
}'

###################################################################################
#       Grant resource link permissions to EMR on EKS Job Execution IAM role
###################################################################################

echo "Grant permission on rl_patients and rl_claims resource links for Team 1"

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME"  \
--permissions "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${CONSUMER_DATABASE}'",
        "Name": "'${rl_patients}'"
    }
}'

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME"  \
--permissions "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${CONSUMER_DATABASE}'",
        "Name": "'${rl_claims}'"
    }
}'

echo "Grant permission on rl_claims resource link for Team 2"

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME"  \
--permissions "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${CONSUMER_DATABASE}'",
        "Name": "'${rl_claims}'"
    }
}'

##############################################################################################################
#       Grant on target from resource link permissions to Consumer account EMR on EKS Job Execution IAM role
################################################################################################################

echo "Grant on target permission on resource link"

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME"  \
--permissions "SELECT" \
--resource '{
    "Table": {
        "DatabaseName": "'${PRODUCER_DATABASE}'",
        "Name":  "'${patients}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME"  \
--permissions "SELECT" "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${PRODUCER_DATABASE}'",
        "Name":  "'${claims}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

aws lakeformation grant-permissions \
--principal DataLakePrincipalIdentifier="arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME"  \
--permissions "SELECT" "DESCRIBE" \
--resource '{
    "Table": {
        "DatabaseName": "'${PRODUCER_DATABASE}'",
        "Name":  "'${claims}'",
        "CatalogId": "'${PRODUCER_AWS_ACCOUNT}'"
    }
}'

#############################################################################################################
#       Create Security Configuration
###############################################################################################################

security_config_id=$(aws emr-containers create-security-configuration \
    --name "$SECURITY_CONFIG_NAME" \
    --security-configuration '{
        "authorizationConfiguration": {
            "lakeFormationConfiguration": {
                "authorizedSessionTagValue": "EMR on EKS Engine",
                "secureNamespaceInfo": {
                    "clusterId": "'$EKSCLUSTER_NAME'",
                    "namespace": "'$EMR_LF_SECURE_NAMESPACE'"
                },
                "queryEngineRoleArn": "arn:aws:iam::'${CONSUMER_AWS_ACCOUNT}':role/'$QUERY_ROLE_NAME'"
            }
        }
    }'| jq '.id' -r)

echo $security_config_id

aws emr-containers create-virtual-cluster \
--name "$EMR_VC_NAME" \
--container-provider '{
    "id": "'$EKSCLUSTER_NAME'",
    "type": "EKS",
    "info": {
        "eksInfo": {
            "namespace": "'$EMR_LF_USER_NAMESPACE'"
        }
    }
}' \
--security-configuration-id "$security_config_id"

export VCID=$(aws emr-containers list-virtual-clusters \
--query 'virtualClusters[?name==`'$EMR_VC_NAME'` && state==`RUNNING`] | [0].id' \
--output text)

echo $VCID

################################################################################################################
#       Patients: Create PySpark job code file & upload it to S3
################################################################################################################

cat <<EOF >/tmp/cross-account-patient-job.py
import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CrossAccountTeam1EMRonEKSJob")\
      .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
      .enableHiveSupport()\
      .getOrCreate()

    print("Patient Care Team PySpark job running on EMR on EKS to query Patients and Claims tables!")
    print("This job queries Patients and Claims tables!")
    df1 = spark.sql('SELECT * FROM dev.${CONSUMER_DATABASE}.${rl_patients}')
    print("Patients tables data:")
    print("Note: Patients table is filtered out SSN column and it only shows records for Texas and New York states based on our FGAC setup!")
    df1.show(20)

    df2 = spark.sql('SELECT p.state, c.claim_id, c.claim_date, p.patient_name, c.diagnosis_code, c.procedure_code, c.amount, c.status, c.provider_id FROM dev.${CONSUMER_DATABASE}.${rl_claims} c JOIN dev.${CONSUMER_DATABASE}.${rl_patients} p ON c.patient_id = p.patient_id ORDER BY  p.state, c.claim_date')
    print("Show only relevant Claims data for Patients selected from Texas and New York state:")
    df2.show(20)
    print("Job Complete")

    spark.stop()
EOF

aws --region $AWS_REGION s3 cp /tmp/cross-account-patient-job.py \
s3://$S3_TEST_BUCKET/jobs/

echo "========================================================"
echo "  Patients: Run PySpark job on EMR on EKS Cluster ......"
echo "========================================================"

aws emr-containers start-job-run \
--virtual-cluster-id "$VCID" \
--name "$EMR_ON_EKS_TEAM1_JOB_NAME" \
--execution-role-arn "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM1_JOB_ROLE_NAME" \
--release-label emr-7.7.0-latest \
--job-driver '{    "sparkSubmitJobDriver": {
      "entryPoint": "s3://'$S3_TEST_BUCKET'/jobs/cross-account-patient-job.py",
      "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=4G --conf spark.driver.memory=4G --conf spark.kubernetes.driver.request.cores=1 --conf spark.kubernetes.executor.request.cores=1 --jars local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
    }}' \
--configuration-overrides '{"applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.dev": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.dev.warehouse": "s3://'$S3_DATA_BUCKET'/warehouse",
          "spark.sql.catalog.dev.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.dev.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
          "spark.sql.catalog.dev.client.region": "'$AWS_REGION'",
          "spark.sql.defaultCatalog": "dev",
          "spark.sql.catalog.dev.type": "glue",
          "spark.sql.catalog.dev.glue.id": "'$CONSUMER_AWS_ACCOUNT'",
          "spark.sql.catalog.dev.glue.account-id": "'$CONSUMER_AWS_ACCOUNT'"
        }
      }
    ],
     "monitoringConfiguration":
      {
        "persistentAppUI": "ENABLED",
        "s3MonitoringConfiguration": {"logUri": "s3://'$S3_TEST_BUCKET'/spark-logs/"}
      }
    }'



##############################################################################################################
#       Claims: Create PySpark job code file & upload it to S3
################################################################################################################
#

cat <<EOF >/tmp/cross-account-claims-job.py
import sys
import logging
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CrossAccountTeam2EMRonEKSJob")\
      .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
      .enableHiveSupport()\
      .getOrCreate()
    print("Claims Team PySpark job running on EMR on EKS to query Claims table!")
    print("Note: Claim team only has access to Claims table not Patients table!")
    df = spark.sql('SELECT * FROM dev.${CONSUMER_DATABASE}.${rl_claims}')
    df.show(20)
    try:
      df1 = spark.sql('SELECT * FROM dev.${CONSUMER_DATABASE}.${rl_patients}')
      df1.show(20)
    except Exception as e:
    # Catch the AccessDeniedException or a more general Exception
      if "AccessDeniedException" in str(e):
          logging.error(f"Access Denied Error! Please check your Lake Formation permission(s) on ${rl_patients} table.")
      else:
          logging.error(f"An unexpected error occurred: {e}")
    finally:
      spark.stop()

    print("Job Complete")

    spark.stop()
EOF

aws --region $AWS_REGION s3 cp /tmp/cross-account-claims-job.py \
s3://$S3_TEST_BUCKET/jobs/

echo "======================================================"
echo "  Claims: Run PySpark job on EMR on EKS Cluster ......"
echo "======================================================"

aws emr-containers start-job-run \
--virtual-cluster-id "$VCID" \
--name "$EMR_ON_EKS_TEAM2_JOB_NAME" \
--execution-role-arn "arn:aws:iam::${CONSUMER_AWS_ACCOUNT}:role/$TEAM2_JOB_ROLE_NAME" \
--release-label emr-7.7.0-latest \
--job-driver '{    "sparkSubmitJobDriver": {
      "entryPoint": "s3://'$S3_TEST_BUCKET'/jobs/cross-account-claims-job.py",
      "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=4G --conf spark.driver.memory=4G --conf spark.kubernetes.driver.request.cores=1 --conf spark.kubernetes.executor.request.cores=1 --jars local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
    }}' \
--configuration-overrides '{"applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.dev": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.dev.warehouse": "s3://'$S3_DATA_BUCKET'/warehouse",
          "spark.sql.catalog.dev.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.dev.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
          "spark.sql.catalog.dev.client.region": "'$AWS_REGION'",
          "spark.sql.defaultCatalog": "dev",
          "spark.sql.catalog.dev.type": "glue",
          "spark.sql.catalog.dev.glue.id": "'$CONSUMER_AWS_ACCOUNT'",
          "spark.sql.catalog.dev.glue.account-id": "'$CONSUMER_AWS_ACCOUNT'"
        }
      }
    ],
     "monitoringConfiguration":
      {
        "persistentAppUI": "ENABLED",
        "s3MonitoringConfiguration": {"logUri": "s3://'$S3_TEST_BUCKET'/spark-logs/"}
      }
    }'

