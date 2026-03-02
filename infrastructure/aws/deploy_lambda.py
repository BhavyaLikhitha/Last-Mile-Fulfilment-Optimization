"""
Deploy Lambda + EventBridge
Packages the simulation code, creates the Lambda function,
and sets up EventBridge rules for daily and weekly triggers.

Usage:
    python -m infrastructure.aws.deploy_lambda              # deploy/update everything
    python -m infrastructure.aws.deploy_lambda --mode daily   # enable daily schedule
    python -m infrastructure.aws.deploy_lambda --mode weekly  # enable weekly schedule
    python -m infrastructure.aws.deploy_lambda --mode disable # disable all schedules
    python -m infrastructure.aws.deploy_lambda --trigger      # manual trigger (1 day)
    python -m infrastructure.aws.deploy_lambda --trigger --days 7  # manual trigger 7 days

Prerequisites:
    AWS user needs: AWSLambda_FullAccess, AmazonEventBridgeFullAccess, IAMFullAccess
"""

import os
import sys
import json
import time
import boto3
import zipfile
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────
AWS_REGION      = os.getenv('AWS_REGION', 'us-east-2')
S3_BUCKET       = os.getenv('S3_BUCKET_NAME', 'last-mile-fulfillment-platform')
LAMBDA_NAME     = 'fulfillment-data-generator'
LAMBDA_ROLE     = 'fulfillment-lambda-role'
LAMBDA_TIMEOUT  = 900   # 15 minutes (Lambda max)
LAMBDA_MEMORY   = 3008  # MB — needed for pandas/numpy simulation

# EventBridge rule names
DAILY_RULE_NAME  = 'fulfillment-daily'
WEEKLY_RULE_NAME = 'fulfillment-weekly'

# Daily: every day at 2am UTC
# Weekly: every Monday at 3am UTC
DAILY_SCHEDULE  = 'cron(0 2 * * ? *)'
WEEKLY_SCHEDULE = 'cron(0 3 ? * MON *)'

# Packages to include in Lambda zip
INCLUDE_DIRS = [
    'config',
    'data_simulation',
]
INCLUDE_FILES = [
    'data_simulation/lambda_handler.py',
]


def get_clients():
    session = boto3.Session(
        aws_access_key_id    =os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name          =AWS_REGION
    )
    return (
        session.client('lambda'),
        session.client('iam'),
        session.client('events'),
        session.client('sts')
    )


# ── Step 1: IAM Role ─────────────────────────────────────────

def create_lambda_role(iam) -> str:
    """Create Lambda execution role with S3 access. Returns role ARN."""
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect"   : "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action"   : "sts:AssumeRole"
        }]
    }

    try:
        response = iam.create_role(
            RoleName                =LAMBDA_ROLE,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description             ='Execution role for fulfillment data generator Lambda'
        )
        role_arn = response['Role']['Arn']
        print(f"  Created IAM role: {LAMBDA_ROLE}")
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=LAMBDA_ROLE)['Role']['Arn']
        print(f"  IAM role already exists: {LAMBDA_ROLE}")

    # Attach policies
    policies = [
        'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
        'arn:aws:iam::aws:policy/AmazonS3FullAccess',
    ]
    for policy in policies:
        try:
            iam.attach_role_policy(RoleName=LAMBDA_ROLE, PolicyArn=policy)
        except Exception:
            pass  # Already attached

    print(f"  Role ARN: {role_arn}")
    print("  Waiting 10s for role to propagate...")
    time.sleep(10)
    return role_arn


# ── Step 2: Package Lambda ────────────────────────────────────

def create_deployment_package() -> bytes:
    """
    Create a ZIP file containing:
      - lambda_handler.py (entry point)
      - config/ (constants, warehouse_config)
      - data_simulation/ (all simulation modules)
      - numpy, pandas, boto3 bundled as dependencies

    Lambda base runtime does not include numpy/pandas so they must
    be bundled in the deployment package.
    """
    import io
    import sys
    import subprocess
    import tempfile

    zip_buffer   = io.BytesIO()
    project_root = Path(__file__).parent.parent.parent
    print(f"  Project root: {project_root}")

    # Install dependencies into a temp directory then zip them
    print("  Installing dependencies into package (numpy, pandas)...")
    print("  This takes 2-3 minutes on first run...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        deps_dir = Path(tmp_dir) / 'deps'
        deps_dir.mkdir()

        subprocess.run([
            sys.executable, '-m', 'pip', 'install',
            'numpy', 'pandas', 'python-dateutil', 'pytz',
            '--target', str(deps_dir),
            '--quiet',
            '--upgrade',
            '--platform', 'manylinux2014_x86_64',
            '--implementation', 'cp',
            '--python-version', '3.12',
            '--only-binary=:all:',
        ], check=True)

        files_added = 0
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:

            # Bundle dependencies
            skip_patterns = ['__pycache__', '.dist-info', '.egg-info', 'test/', 'tests/']
            for f in deps_dir.rglob('*'):
                if f.is_file() and not any(p in str(f) for p in skip_patterns):
                    arcname = str(f.relative_to(deps_dir)).replace('\\', '/')
                    zf.write(f, arcname)
                    files_added += 1

            # Add lambda_handler.py
            handler_path = project_root / 'data_simulation' / 'lambda_handler.py'
            if handler_path.exists():
                zf.write(handler_path, 'lambda_handler.py')
                files_added += 1
            else:
                raise FileNotFoundError(f"lambda_handler.py not found at {handler_path}")

            # Add config/
            config_dir = project_root / 'config'
            for f in config_dir.rglob('*.py'):
                arcname = str(f.relative_to(project_root)).replace('\\', '/')
                zf.write(f, arcname)
                files_added += 1

            # Add data_simulation/
            sim_dir = project_root / 'data_simulation'
            for f in sim_dir.rglob('*.py'):
                arcname = str(f.relative_to(project_root)).replace('\\', '/')
                zf.write(f, arcname)
                files_added += 1

    zip_bytes = zip_buffer.getvalue()
    size_mb   = len(zip_bytes) / (1024 * 1024)
    print(f"  Files added : {files_added}")
    print(f"  Package size: {size_mb:.1f} MB")
    return zip_bytes


# ── Step 3: Deploy Lambda ─────────────────────────────────────

def deploy_lambda(lambda_client, role_arn: str, zip_bytes: bytes):
    """Create or update the Lambda function."""
    env_vars = {
        'S3_BUCKET_NAME': S3_BUCKET,
        'APP_AWS_REGION': AWS_REGION,  # AWS_DEFAULT_REGION is reserved by Lambda
    }

    try:
        # Try to update existing function
        lambda_client.update_function_code(
            FunctionName=LAMBDA_NAME,
            ZipFile     =zip_bytes,
        )
        # Wait for code update to complete before updating configuration
        print("  Waiting for code update to complete...")
        waiter = lambda_client.get_waiter('function_updated_v2')
        waiter.wait(FunctionName=LAMBDA_NAME)
        lambda_client.update_function_configuration(
            FunctionName =LAMBDA_NAME,
            Timeout      =LAMBDA_TIMEOUT,
            MemorySize   =LAMBDA_MEMORY,
            Environment  ={'Variables': env_vars},
        )
        print(f"  Updated Lambda function: {LAMBDA_NAME}")

    except lambda_client.exceptions.ResourceNotFoundException:
        # Create new function
        lambda_client.create_function(
            FunctionName =LAMBDA_NAME,
            Runtime      ='python3.12',
            Role         =role_arn,
            Handler      ='lambda_handler.lambda_handler',
            Code         ={'ZipFile': zip_bytes},
            Timeout      =LAMBDA_TIMEOUT,
            MemorySize   =LAMBDA_MEMORY,
            Environment  ={'Variables': env_vars},
            Description  ='Fulfillment platform incremental data generator',
        )
        print(f"  Created Lambda function: {LAMBDA_NAME}")

    # Wait for function to be active
    print("  Waiting for Lambda to be active...")
    waiter = lambda_client.get_waiter('function_active_v2')
    waiter.wait(FunctionName=LAMBDA_NAME)
    print("  Lambda is active")

    return lambda_client.get_function(FunctionName=LAMBDA_NAME)['Configuration']['FunctionArn']


# ── Step 4: EventBridge Rules ─────────────────────────────────

def setup_eventbridge(events_client, lambda_client, lambda_arn: str, mode: str = 'daily'):
    """
    Create EventBridge rules for daily and weekly triggers.
    mode: 'daily' enables daily, 'weekly' enables weekly, 'disable' disables all
    """
    rules = [
        {
            'name'     : DAILY_RULE_NAME,
            'schedule' : DAILY_SCHEDULE,
            'payload'  : {'mode': 'daily'},
            'enabled'  : mode == 'daily',
            'desc'     : 'Daily at 2am UTC — generates 1 day of fulfillment data'
        },
        {
            'name'     : WEEKLY_RULE_NAME,
            'schedule' : WEEKLY_SCHEDULE,
            'payload'  : {'mode': 'weekly'},
            'enabled'  : mode == 'weekly',
            'desc'     : 'Weekly Monday 3am UTC — generates 7 days of fulfillment data'
        }
    ]

    for rule in rules:
        state = 'ENABLED' if rule['enabled'] else 'DISABLED'

        # Create/update rule
        response = events_client.put_rule(
            Name               =rule['name'],
            ScheduleExpression =rule['schedule'],
            State              =state,
            Description        =rule['desc'],
        )
        rule_arn = response['RuleArn']

        # Add Lambda target
        events_client.put_targets(
            Rule   =rule['name'],
            Targets=[{
                'Id'   : '1',
                'Arn'  : lambda_arn,
                'Input': json.dumps(rule['payload'])
            }]
        )

        # Grant EventBridge permission to invoke Lambda
        try:
            lambda_client.add_permission(
                FunctionName =LAMBDA_NAME,
                StatementId  =f'eventbridge-{rule["name"]}',
                Action       ='lambda:InvokeFunction',
                Principal    ='events.amazonaws.com',
                SourceArn    =rule_arn,
            )
        except lambda_client.exceptions.ResourceConflictException:
            pass  # Permission already exists

        status = "ENABLED" if rule['enabled'] else "disabled"
        print(f"  {rule['name']}: {status} ({rule['schedule']})")


# ── Step 5: Manual Trigger ────────────────────────────────────

def trigger_lambda(lambda_client, days: int = 1, mode: str = 'manual'):
    """Manually invoke the Lambda function."""
    payload = {'mode': mode, 'days': days} if mode == 'manual' else {'mode': mode}

    print(f"\n  Triggering Lambda: mode={mode}, days={days}")
    print(f"  Payload: {json.dumps(payload)}")

    response = lambda_client.invoke(
        FunctionName   =LAMBDA_NAME,
        InvocationType ='RequestResponse',  # synchronous
        Payload        =json.dumps(payload).encode()
    )

    result = json.loads(response['Payload'].read().decode('utf-8'))

    if response['StatusCode'] == 200:
        print(f"\n  Lambda completed successfully!")
        print(f"  Generated: {result.get('start_date')} → {result.get('end_date')}")
        print(f"  Total rows: {result.get('total_rows', 0):,}")
        print(f"  Duration: {result.get('duration_s', 0)}s")
    else:
        print(f"  Lambda failed: {result}")

    return result


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Deploy Lambda + EventBridge for fulfillment platform')
    parser.add_argument('--mode',    choices=['daily', 'weekly', 'disable'], default='daily',
                        help='EventBridge schedule to enable (default: daily)')
    parser.add_argument('--trigger', action='store_true',
                        help='Manually trigger Lambda after deployment')
    parser.add_argument('--days',    type=int, default=1,
                        help='Number of days to generate on manual trigger (default: 1)')
    parser.add_argument('--trigger-only', action='store_true',
                        help='Skip deployment, just trigger existing Lambda')
    args = parser.parse_args()

    lambda_client, iam, events_client, sts = get_clients()

    # Verify credentials
    identity = sts.get_caller_identity()
    print(f"  AWS Account: {identity['Account']}")
    print(f"  Region     : {AWS_REGION}")

    if not args.trigger_only:
        print("\n[1/4] Creating IAM role...")
        role_arn = create_lambda_role(iam)

        print("\n[2/4] Packaging Lambda code...")
        zip_bytes = create_deployment_package()

        print("\n[3/4] Deploying Lambda function...")
        lambda_arn = deploy_lambda(lambda_client, role_arn, zip_bytes)
        print(f"  Lambda ARN: {lambda_arn}")

        print(f"\n[4/4] Setting up EventBridge rules (mode: {args.mode})...")
        setup_eventbridge(events_client, lambda_client, lambda_arn, args.mode)

        print("\n  Deployment complete!")
        print(f"  Lambda function : {LAMBDA_NAME}")
        print(f"  Daily rule      : {DAILY_RULE_NAME} ({'ENABLED' if args.mode=='daily' else 'disabled'})")
        print(f"  Weekly rule     : {WEEKLY_RULE_NAME} ({'ENABLED' if args.mode=='weekly' else 'disabled'})")
    else:
        lambda_arn = lambda_client.get_function(FunctionName=LAMBDA_NAME)['Configuration']['FunctionArn']
        print(f"  Using existing Lambda: {lambda_arn}")

    if args.trigger or args.trigger_only:
        print("\n  Triggering Lambda manually...")
        trigger_lambda(lambda_client, days=args.days, mode='manual')

    print("\nDone.")


if __name__ == '__main__':
    main()