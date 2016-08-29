create-profile(){
	h=`aws iam list-roles|grep RoleName |grep dev-ops |cut -d '"' -f 4`
	if [ -z $h ]; then
	    echo "creating dev-ops role"
	    aws iam create-instance-profile --instance-profile-name dev-ops
	    aws iam create-role --role-name dev-ops --assume-role-policy-document '{             "Version": "2012-10-17",             "Statement": [                 {                     "Action": "sts:AssumeRole",                     "Principal": {                        "Service": "ec2.amazonaws.com"                     },                     "Effect": "Allow",                     "Sid": ""                 }             ]         }'
	    aws iam add-role-to-instance-profile --instance-profile-name dev-ops --role-name dev-ops
	    aws iam put-role-policy --role-name dev-ops --policy-name 'AllowEc2forDevOps' --policy-document '{       "Version": "2012-10-17",       "Statement": [         {           "Action": "ec2:*",           "Effect": "Allow",           "Resource": "*"         }       ]     }'
	    aws iam put-role-policy --role-name dev-ops --policy-name 'AllowS3forDevOps'  --policy-document '{       "Version": "2012-10-17",       "Statement": [         {           "Action": "s3:*",           "Effect": "Allow",           "Resource": "*"         }       ]     }'
	    aws iam put-role-policy --role-name dev-ops --policy-name 'AllowPassRoleforDevOps'  --policy-document '{       "Version": "2012-10-17",       "Statement": [         {           "Sid": "Stmt1409776891000",           "Effect": "Allow",           "Action": [             "iam:PassRole"           ],           "Resource": [             "*"           ]         }       ]     }'
	else 
	    echo "dev-ops role already exits"
	fi
}
create-profile

ansible-playbook -i inventory/local iuberdata-prov.yml
ansible-playbook -i inventory/hosts iuberdata.yml
