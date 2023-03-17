# Introduction
A lakehouse project using AWS Glue, PySpark, and Athena

# Set up 
We first need to create S3 directories for our raw data. These will be called

* `customer_landing`
* `step_trainer_landing`
* `accelerometer_landing`

We'll create an S3 bucket called `stedi-human-balance-analytics-mw`. The raw data, all in JSON format, is in `data/`. We have a little shell script, `s3-sync-all.sh`, which will put all of this data in our folders with a single command. We'll `export` our temporary credentials and run the shell script like `./data/s3-sync-all.sh`. [1]

# Inspecting the data
With the data in S3, we'll have an initial look at it. We'll create two Glue Tables: `customer_landing` and `accelerometer_landing` in a database called `stedi-human-balance-analytics`. We'll point Glue at the S3 folders we've created, e.g. `customer/landing/`, and define the schema. 

In raw format, one record of the customer data looks like this:

```
{
    "customerName": "Frank Doshi",
    "email": "Frank.Doshi@test.com",
    "phone": "8015551212",
    "birthDay": "1965-01-01",
    "serialNumber": "159a908a-371e-40c1-ba92-dcdea483a6a2",
    "registrationDate": 1655293787680,
    "lastUpdateDate": 1655293823654,
    "shareWithResearchAsOfDate": 1655293823654,
    "shareWithPublicAsOfDate": 1655293823654
}
```


# Footnotes 
[1] There is about 200 MB of data, which is normally far too much to keep in a git repository, but for the purpose of this being a self-contained project, I'll put the data in here.