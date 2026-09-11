# Glue CSV Workflow — CloudFormation Edition

Recreates the classic S3 → Glue Crawler → Glue Job (CSV transform) → S3
pipeline, but instead of clicking through the console, everything is defined
as **code** in `template.yaml` and deployed as one AWS CloudFormation stack.

| Old concept (console)      | This project's name      |
|-----------------------------|---------------------------|
| Upload bucket                | `upload-csv-cf`           |
| Destination bucket           | `destination-csv-cf`      |
| Glue Database                | `csv_database_cf`         |
| Glue Crawler                 | `csv-crawler-cf`          |
| Glue Job                     | `csv-transform-job-cf`    |
| Glue Workflow                | `csv-workflow-cf`         |
| Start trigger (runs crawler) | `start-crawler-trigger-cf`|
| Conditional trigger (runs job)| `start-job-trigger-cf`   |
| IAM Role for Glue             | `glue-service-role-cf`   |

## Repository layout

```
glue-workflow-cf/
├── template.yaml                    # Main CloudFormation template (S3, IAM, Glue, Lambda automation)
├── github-iam-user.yaml            # IAM user + scoped policy for GitHub Actions (static key auth)
├── .github/
│   └── workflows/
│       └── upload-csv.yml          # GitHub Actions: uploads CSVs to S3 on push
├── glue-scripts/
│   └── csv_transform_job.py        # Python Shell script the Glue Job runs
├── sample-data/
│   └── sample.csv                  # Drop new/updated CSVs here to trigger the pipeline via git push
└── README.md
```

#### Architecture

![architecture](screenshots/architecture.png)

---

## Part 1 — Theory: what is CloudFormation, actually?

*Concepts:*

- **Template**: a YAML (or JSON) text file that *declares* what AWS resources
  you want (buckets, roles, Glue jobs, etc.) and their configuration. You
  describe the *end state*, not the steps to get there. This is called
  "declarative" infrastructure, as opposed to writing a script that calls
  the AWS API line by line ("imperative").
- **Stack**: when you hand a template to CloudFormation, it creates a
  **stack** which is a single, trackable unit that groups all the resources the
  template defines. Delete the stack, and (by default) every resource it
  created is deleted too. This is the single biggest reason to use
  CloudFormation: no orphaned resources, no manual cleanup.
- **Resources**: each block under `Resources:` in the template maps to one
  real AWS object (`AWS::S3::Bucket`, `AWS::Glue::Job`, etc.). CloudFormation
  figures out the order to create them in based on references between them.
- **Parameters**: the "inputs" of the template (e.g. bucket names). Instead
  of hardcoding a name inside the template, you expose it as a parameter so
  the same template can be reused with different values.
- **Intrinsic functions**: helpers like `!Ref`, `!Sub`, `!GetAtt` that let
  resources refer to each other's values. `!Ref MyBucket` gets the bucket's
  name; `!GetAtt MyRole.Arn` gets a specific attribute (the role's ARN);
  `!Sub` lets you build a string with a variable embedded in it, like a
  full S3 path.
- **Outputs**: values CloudFormation prints out once the stack finishes,
  useful for grabbing generated names/ARNs without hunting through consoles.
- **Drift**: if someone manually edits a resource CloudFormation created
  (e.g. changes a bucket setting by hand in the console), the stack's
  tracked state and reality diverge — this is called "drift." Best practice
  is to only change stack-managed resources by updating the template.

### Automating it: upload a CSV, the whole pipeline runs

By default a Glue Workflow only starts when something explicitly starts it
(console button or `aws glue start-workflow-run`). To make the pipeline
truly event-driven — upload a file, everything else happens on its own —
this template wires up:

```
S3 PutObject event               (new .csv under upload-csv-cf/data/)
        │
        ▼
S3 Notification Configuration    (filters to prefix "data/", suffix ".csv")
        │
        ▼
Lambda: start-csv-workflow-cf    (checks nothing's already running, then
        │                        calls glue:StartWorkflowRun)
        ▼
csv-workflow-cf                  (crawler -> job)
```

Two AWS concepts make this work:
- **S3 Event Notifications**: an S3 bucket can be configured to call a
  Lambda function, SQS queue, or SNS topic whenever objects are
  created/deleted/etc. Here it's scoped with a `Filter` so it only fires
  for objects under the `data/` prefix ending in `.csv` — uploads
  elsewhere in the bucket (like the `scripts/` or `temp/` prefixes) don't
  trigger it.
- **Resource-based permissions**: Lambda functions reject invocations from
  anything not explicitly allowed to call them. `AllowS3InvokeLambdaCF`
  (an `AWS::Lambda::Permission`) is what grants the S3 service permission
  to invoke this specific function — this is separate from the Lambda's
  *execution* role (which controls what the function itself is allowed to
  do once it's running).

The Lambda's own IAM role is scoped narrowly: it can only call
`glue:StartWorkflowRun` / `glue:GetWorkflowRuns` on this one workflow, and
write its own CloudWatch logs — nothing more.

### Automating it further: push a CSV to GitHub, the pipeline runs

The S3 → Lambda → Glue chain above still requires *something* to put the
CSV into S3. This project adds one more layer so that source is GitHub
itself:

```
git push (CSV under sample-data/ changed)
        │
        ▼
GitHub Actions workflow (.github/workflows/upload-csv.yml)
        │  authenticates via a static IAM user access key
        │  (stored as GitHub Actions secrets)
        ▼
aws s3 sync sample-data/ → s3://upload-csv-cf/data/
        │
        ▼
(same S3 notification → Lambda → Glue chain)
```

Nothing about the S3/Lambda/Glue side changes — GitHub Actions' only job is
to become "the thing that uploads the file," using the same trigger path.

#### Authenticating GitHub Actions to AWS: IAM user + access key

This project authenticates GitHub Actions using a **static IAM user access
key** — simpler to set up than OIDC federation, at the cost of the key
being long-lived (it doesn't expire on its own; you rotate it yourself).
The user's permissions are scoped as tightly as the task needs: it can only
`s3:PutObject` into `upload-csv-cf/data/*` and `s3:ListBucket` on that one
bucket — nothing else, in case the key is ever exposed.

#### Setup: Step 1 — deploy the IAM user stack

```bash
aws cloudformation create-stack \
  --stack-name github-iam-user-cf \
  --template-body file://github-iam-user.yaml \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation wait stack-create-complete --stack-name github-iam-user-cf
```

#### Setup: Step 2 — generate an access key via CLI

Deliberately done via CLI rather than in the CloudFormation template
itself. This way the secret key is only ever printed on the terminal
once and it is not stored anywhere in CloudFormation's stack outputs or event
history.

```bash
aws iam create-access-key --user-name github-actions-upload-user-cf
```

![aws iam create-access-key command output](screenshots/terminal5.png)

This prints a JSON block containing `AccessKeyId` and `SecretAccessKey`.
**Copy both immediately** — the secret key is shown only this one time; if
you lose it, delete that key (`aws iam delete-access-key`) and generate a
new one.

#### Setup: Step 3 — add GitHub repo secrets and variables

In your repo: **Settings → Secrets and variables → Actions**.

On the **Secrets** tab (these are sensitive - only the key pair needs to be
here), add two:
- `AWS_ACCESS_KEY_ID` — the `AccessKeyId` from Step 2
- `AWS_SECRET_ACCESS_KEY` — the `SecretAccessKey` from Step 2

On the **Variables** tab (not secret - just config the workflow reads),
add two:
- `AWS_REGION` — the region your stack is deployed in, e.g. `us-east-1`
- `UPLOAD_BUCKET_NAME` — must match the `UploadBucketName` parameter you
  deployed `template.yaml` with, e.g. `upload-csv-cf`. Keeping this as a
  variable instead of hardcoding it in `upload-csv.yml` means renaming the
  bucket only requires updating this one value.

#### Setup: Step 4 — push and watch it run

```bash
git add sample-data/sample.csv
git commit -m "Trigger pipeline via GitHub Actions"
git push origin main
```
The workflow only fires on pushes that touch a `.csv` under `sample-data/`
(see the `paths` filter in `upload-csv.yml`) — editing the README or
templates won't trigger it.

Watch it in the **Actions** tab of your repo

![GitHub Actions deploy pipeline run succeeded](screenshots/github-deploy.png)

Once the Action completes, the same S3 → Lambda → Glue chain you already
verified takes over:
```bash
aws logs tail /aws/lambda/start-csv-workflow-cf --since 5m --follow
aws glue get-workflow-runs --name csv-workflow-cf --max-results 1
```

![workflow detail](screenshots/terminal7.png)

#### Note: Key rotation

Since this key never expires automatically, put a reminder somewhere (a
calendar note is fine) to rotate it periodically:
```bash
# create a new key, update the GitHub secrets with it, THEN deactivate/delete the old one
aws iam create-access-key --user-name github-actions-upload-user-cf
aws iam update-access-key --user-name github-actions-upload-user-cf --access-key-id <OLD_KEY_ID> --status Inactive
# once you've confirmed the new key works in Actions:
aws iam delete-access-key --user-name github-actions-upload-user-cf --access-key-id <OLD_KEY_ID>
```

### Why a Glue *Workflow* specifically?

A single Glue Job can't (on its own) know "wait until the table schema is
up to date before running." A **Workflow** is Glue's orchestration layer: it
strings together **Triggers**, **Crawlers**, and **Jobs** into one pipeline
with dependencies:

```
[start-crawler-trigger-cf]  (ON_DEMAND — you start it manually)
        │
        ▼
[csv-crawler-cf]  (scans upload-csv-cf/data/, updates the Glue Catalog table)
        │
        ▼ (only if crawler SUCCEEDED)
[start-job-trigger-cf]  (CONDITIONAL trigger)
        │
        ▼
[csv-transform-job-cf]  (reads the table, transforms, writes to destination-csv-cf)
```

This mirrors exactly what you'd have built by hand in the console, just
captured as code.

---

## Part 2 — Prerequisites

1. An AWS account with permissions to create S3 buckets, IAM roles, and Glue
   resources.
2. [AWS CLI installed and configured](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
   (`aws configure`, using an IAM user/role with sufficient permissions) —
   only needed if you deploy via CLI rather than the console.
3. Git, and a new empty GitHub repository to push this project into.

---

## Part 3 — Step-by-step deployment

### Step 1: Create your GitHub repo and add these files

```bash
git init glue-workflow-cf
cd glue-workflow-cf
# copy in template.yaml, glue-scripts/, sample-data/, README.md
git add .
git commit -m "Initial commit: Glue CSV workflow via CloudFormation"
git branch -M main
git remote add origin https://github.com/<your-username>/glue-workflow-cf.git
git push -u origin main
```

*Why this matters:* keeping the template in version control means every
change to your infrastructure is tracked, reviewable, and revertible — the
same discipline you'd apply to application code ("Infrastructure as Code").

### Step 2: Deploy the CloudFormation stack

**AWS CLI:**
```bash
aws cloudformation create-stack \
  --stack-name glue-csv-workflow-cf \
  --template-body file://template.yaml \
  --capabilities CAPABILITY_NAMED_IAM

# poll until it finishes:
aws cloudformation wait stack-create-complete --stack-name glue-csv-workflow-cf
```
`--capabilities CAPABILITY_NAMED_IAM` is required because the template
creates an IAM role with an explicit name (`glue-service-role-cf`) —
CloudFormation requires this explicit acknowledgment as a safety check
before it's allowed to create IAM resources.

![create-stack command output](screenshots/terminal1.png)

You can spot-check that the Job resource picked up the right arguments:

```bash
aws glue get-job --job-name csv-transform-job-cf --query "Job.DefaultArguments"
```

![Job DefaultArguments query output](screenshots/terminal4.png)

### Step 3: Upload the Glue script to the newly-created upload bucket

The Job resource pointed at `s3://upload-csv-cf/scripts/csv_transform_job.py`
before that file existed — CloudFormation doesn't check the file is there at
deploy time, only Glue checks it when the job actually *runs*. So now that
the bucket exists, upload the script:

```bash
aws s3 cp glue-scripts/csv_transform_job.py s3://upload-csv-cf/scripts/csv_transform_job.py
```

![Uploading the Glue script via aws s3 cp](screenshots/terminal2.png)

### Step 4: Upload sample data

The crawler is configured to scan `s3://upload-csv-cf/data/`:

```bash
aws s3 cp sample-data/sample.csv s3://upload-csv-cf/data/sample.csv
```

![Uploading sample data via aws s3 cp](screenshots/terminal3.png)

### Step 5: Run the workflow

The S3 → Lambda → Glue automation was already created along with everything
else back in Step 2 — there's no separate step to "turn it on." **You don't
need to manually start anything**: uploading a CSV under `data/` is enough:
```bash
aws s3 cp sample-data/sample.csv s3://upload-csv-cf/data/sample2.csv
```
Within a few seconds, S3 invokes the Lambda, which starts the workflow. You
can watch it happen:
```bash
aws glue get-workflow-runs --name csv-workflow-cf --max-results 1
```

![CLI output showing a COMPLETED workflow run](screenshots/terminal6.png)

Or watch it in the console — every step in the graph turns green as it
completes:

![Glue Workflow graph with every step succeeded](screenshots/success-workflow.png)

![csv-workflow-cf workflow details and run history in the console](screenshots/workflow-completed.png)

or check the Lambda's own logs to confirm it fired:
```bash
aws logs tail /aws/lambda/start-csv-workflow-cf --since 5m --follow
```

**Manual start still works too** (useful for testing without uploading a
new file):
- **Console:** Go to **AWS Glue** → **Workflows** → select `csv-workflow-cf` →
  **Run**.
- **CLI:**
```bash
aws glue start-workflow-run --name csv-workflow-cf
```

### Step 6: Verify the output

```bash
aws s3 ls s3://destination-csv-cf/processed/
```
![verification output](screenshots/output-verification.png)

In the console, the job's run history should show `Succeeded`:

![csv-transform-job-cf run succeeded in the console](screenshots/job-success.png)

![Job run details page showing run status, duration, and input arguments](screenshots/job-run-details.png)

The job's CloudWatch log group (`/aws-glue/python-jobs/output`) shows the
script's own `print()` output, confirming exactly what it read and wrote:

![CloudWatch log events from the Python Shell job run](screenshots/log-events.png)

### Step 7: Tear it down (avoid ongoing charges)

Both buckets have versioning enabled, so `aws s3 rm --recursive` is **not**
enough to empty them: it only deletes the current version of each object
(actually just adding a delete marker on top of a versioned object) — the
older versions and delete markers stay behind. CloudFormation refuses to
delete a bucket unless every version and delete marker in it is gone too,
so if you only run `s3 rm --recursive` the stack deletion will fail on the
bucket resources with a `BucketNotEmpty` error.

Purge every version using only the AWS CLI (no `jq`, no `python`, and using a
local file rather than `/tmp` — on Windows with the native AWS CLI exe,
Git Bash's `/tmp` isn't a path it can resolve):
```bash
for BUCKET in upload-csv-cf destination-csv-cf; do
  aws s3api list-object-versions --bucket "$BUCKET" --output json \
    --query "{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}" \
    > "./delete-$BUCKET.json"

  if grep -q '"Key"' "./delete-$BUCKET.json"; then
    aws s3api delete-objects --bucket "$BUCKET" --delete "file://delete-$BUCKET.json"
  fi
  rm -f "./delete-$BUCKET.json"
done

aws cloudformation delete-stack --stack-name glue-csv-workflow-cf
aws cloudformation wait stack-delete-complete --stack-name glue-csv-workflow-cf
```

This has been run end-to-end against real versioned objects and
confirmed to fully empty both buckets and let the stack delete cleanly.

*Note*: The AWS Console's **S3 → bucket → Empty** button does the same full-version purge.
---
This is the payoff of using CloudFormation: one command removes every
resource the project created — buckets, IAM role, Glue database, crawler,
job, workflow, and triggers.