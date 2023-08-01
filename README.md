tf-explorer
===========

This tool loads terraform state into an in-memory sqlite database, and allows you to run queries against it.

I built this because I got sick of trawling through the AWS console for discovery,
especially to answer questions like "is there really anything in this VPC?"

To run it you first need terraform state files.  I generate these with [Terraformer](https://github.com/GoogleCloudPlatform/terraformer/blob/master/docs/aws.md)

e.g.

    terraformer-aws import aws --resources=eip,elb,alb,vpc,ec2_instance,ebs,eni,subnet,route_table,sg,transit_gateway --regions=ap-southeast-2,ap-southeast-1 --profile=...

which produces a bunch of tf and tfstate files in generated/aws/{service}/{region}

When you checkout tf-explorer, create a virtualenv

    python3 -m venv venv
    . venv/bin/activate
    pip install -r requirements.txt

then you can run the script

    python3 tf-explorer.py `find ../path-to-terraformer-generated -name terraform.tfstate`

The database has a table per terraform resource type, columns named after the metadata values found in the terraform state.

    select aws_subnet.id, aws_subnet.availability_zone, cidr_block, map_public_ip_on_launch, count(aws_instance.id) from aws_subnet left outer join aws_instance on aws_subnet.id = aws_instance.subnet_id where aws_instance.instance_state = 'running' group by aws_subnet.id;

There are some extra commands: ".tab" lists tables, ".schema" lists the schema and ".schema {tablename}" just for one table.

Flags start with a # and can precede a query.  They include "#md" - markdown format the table, "

Some queries, for example on ec2 instances, return a whole load of NULLs.  You can prepend these queries with "#collapse":

    #collapse select * from aws_instance limit 1;

## metacommands

*.tab* lists all the tables

*.cols tablename* lists all the columns in a table, e.g. ``.cols aws_instance``

## Extra SQL functions

*aws_account(arn)* returns the AWS account inside an AWS ARN

*arn_field(arn, fieldno)* returns a field (0-indexed) from inside an AWS ARN.  e.g. ``arn_field(arn,3)`` returns the region

*ip_sortable(ipaddr)* returns a string which will sort in IP address order.  e.g. end your query with ``ORDER BY ip_sortable(cidr_block)`` when sorting subnets or VPCs.

*ip_within(needle, haystack)* returns "is needle within range haystack."  e.g. ``ip_within(private_ip, '100.64.0.0/10')`` to find EC2 instances in CGNAT space.  Note that the SQL LIKE operator is much faster than ip_within!  SQLite's query engine is smart enough that you could do the previous query much more efficiently with ``private_ip LIKE '100.%' AND ip_within(private_ip, '100.64.0.0/10')``

flowparse.py
============

Flowparse parses AWS VPC (and Transit Gateway) flow logs to sqlite files.

Sample usage

    python3 flowparse.py --sqlite-file flows.sqlite ../flow-logs/djg-ftf-flowlogs --flowcache ../flow-logs/cache

Then you can run tf-explorer, and load the flow database into it:

    python3 tf-explorer.py --flowdb ../FlowLogs/combined.db `find ../path-to-terraformer-generated -name terraform.tfstate`

A sample query to find EC2s communicating via their public IPs:

`> #md select src, json_extract(a1.tags, '$.Name') as src_name, dst, json_extract(a2.tags, '$.Name') as dst_name, bytes from flow join aws_instance as a1 on (flow.src=a1.private_ip or flow.src=a1.public_ip) left outer join aws_instance as a2 on (flow.dst=a2.private_ip or flow.dst=a2.public_ip) where (dst like '10.%' or dst like '172.%') group by src order by bytes;`

| src           | src_name | dst       | dst_name | bytes |
|:--------------|:---------|:----------|:---------|:------|
| 10.0.0.72     | ec2-0    | 10.0.0.8  | None     | 185   |
| 172.31.37.231 | ec2-1    | 10.0.0.72 | ec2-0    | 268   |
| 3.25.235.57   | ec2-1    | 10.0.0.72 | ec2-0    | 360   |
