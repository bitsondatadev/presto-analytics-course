For this module you'll need to set up basic AWS S3 bucket. 
You can do this without paying for AWS services by setting up a new AWS account and
verifying you are eligible for free tier. 

Once you have done that, you will need to set up an S3 user and get the access key and 
secret key and replace the following locations:

aws.properties
```text
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
```
This will tell presto how to access the S3 bucket.

core-site.xml
```text
    <property>
        <name>fs.s3a.access.key</name>
        <value>YOUR_ACCESS_KEY</value>
    </property>

    <property>
        <name>fs.s3a.secret.key</name>
        <value>YOUR_SECRET_KEY</value>
    </property>
```
This will tell hive metastore how to access the S3 bucket.