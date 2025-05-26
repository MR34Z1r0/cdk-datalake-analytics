from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_dms as dms,
    aws_ec2 as ec2,
    aws_stepfunctions as sf,
    aws_events as events,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_events_targets as targets,
    aws_stepfunctions_tasks as tasks,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment
)
import aws_cdk as core
from constructs import Construct
from artifacts.code.utils.resource_creation_utils import RESOURCE_CREATOR, SERVICES
from artifacts.code.utils.path_utils import PATH_UTILS
from artifacts.code.utils.policy_utils import POLICY_UTILS
import pandas as pd
import json

class cadena_analytics_stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, props:dict, shared_resources:dict = {}, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        resource_creator = RESOURCE_CREATOR(self, props['PROJECT_NAME'], props['ENVIRONMENT'], props['ENTERPRISE'], props['ARTIFACTS_BUCKET'])
        
        ################################################
        #            EXISTING RESOURCES                #
        ################################################

        artifatcs_bucket  = resource_creator.import_bucket_from_name(props['ARTIFACTS_BUCKET'])
        resource_creator.set_artifacts_bucket(artifatcs_bucket)
        #Subir scripts individuales al bucket
        utils_deployment = s3_deployment.BucketDeployment(
            self, "analytics_common_functions",
            destination_bucket=artifatcs_bucket,
            destination_key_prefix = PATH_UTILS.PATH_AWS_ARTIFCATS_LAYERS,
            prune=False,
            sources=[
                s3_deployment.Source.asset(PATH_UTILS.PATH_LOCAL_ARTIFCATS_LAYERS)
            ],
        )
        csv_deployment = s3_deployment.BucketDeployment(
            self, "analytics_csv",
            destination_bucket=artifatcs_bucket,
            destination_key_prefix = PATH_UTILS.PATH_AWS_ARTIFACTS_CSV,
            prune=False,
            sources=[
                s3_deployment.Source.asset(PATH_UTILS.PATH_LOCAL_ARTIFCATS_CSV)
            ],
        )
        jobs_scripts_deployment = s3_deployment.BucketDeployment(
            self, "jobs-scripts",
            destination_bucket=artifatcs_bucket,
            destination_key_prefix= PATH_UTILS.PATH_AWS_ARTIFCATS_GLUE_JOBS_ANALYTICS,
            prune=False,
            sources=[
                s3_deployment.Source.asset(PATH_UTILS.PATH_LOCAL_ARTIFCATS_GLUE_JOBS_ANALYTICS)
            ],
        )

        stage_bucket = resource_creator.import_bucket_from_name(props['STAGE_BUCKET'])
        analytics_bucket = resource_creator.import_bucket_from_name(props['ANALYTICS_BUCKET'])
        external_bucket = resource_creator.import_bucket_from_name(props['EXTERNAL_FILES_BUCKET'])

        sns_error_topic = resource_creator.import_sns_topic_from_arn(props['ERROR_TOPIC_ARN'])

        dynamodb_credentials_table = resource_creator.import_dynamodb_table_from_name(props['CREDENTIALS_DYNAMO_TABLE'])
        dynamodb_logs_table = resource_creator.import_dynamodb_table_from_name(props['LOGS_DYNAMO_TABLE'])

        ################################################
        #            IAM ROLES AND POLICIES            #
        ################################################

        policies_access_to_dynamo = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "dynamodb:Scan"
            ],
            resources=["*"]
        )

        policies_access_to_s3 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:GetBucketLocation",
                "s3:GetBucketPolicy",
                "s3:GetBucketAcl",
                "s3:GetObjectVersion",
                "s3:List*"
              ],
            resources=[
                artifatcs_bucket.bucket_arn, 
                artifatcs_bucket.bucket_arn  +  "/*",
                stage_bucket.bucket_arn, 
                stage_bucket.bucket_arn  +  "/*",
                analytics_bucket.bucket_arn, 
                analytics_bucket.bucket_arn  +  "/*",
                external_bucket.bucket_arn,
                external_bucket.bucket_arn + "/*"
            ]
        )

        policies_sns_publish = iam.PolicyStatement(
            actions=POLICY_UTILS.SNS_PUBLIS_PERMISSIONS,
            resources=[
                props['ERROR_TOPIC_ARN']
            ]
        )     

        policies_sf_execute_jobs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun",
            ],
            resources=["*"]
        )

        policies_sf_xray = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets",
            ],
            resources=["*"]
        )

        policies_sf_invoke_lambda = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "lambda:InvokeFunction",
            ],
            resources=["*"]
        )

        policies_sf_start_state_machine = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "states:StartExecution",
                "states:StopExecution",
                "states:DescribeExecution",
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule",
            ],
            resources=["*"]
        )

        policies_lambda_get_jobs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:GetJobRuns",
            ],
            resources=[
                "*"
            ]
        )

        policies_lambda_crawler = iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions=[
                "glue:StartCrawler",
                "glue:GetCrawler",
                "glue:GetCrawlers",
                "glue:StopCrawler",
            ],
            resources=[
                "*"
            ]
        )

        glue_role_access = resource_creator.create_iam_role(SERVICES.GLUE_JOB, "analytics")
        glue_role_access.add_to_policy(policies_access_to_s3)
        glue_role_access.add_to_policy(policies_sns_publish)
        glue_role_access.add_to_policy(policies_access_to_dynamo)
        #AWSGlueServiceRole
        glue_role_access.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))

        crawler_role = resource_creator.create_iam_role(SERVICES.GLUE_JOB, "crawler")
        crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"))
        crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin"))

        state_machine_role_base = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_base")
        state_machine_role_base.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_base.add_to_policy(policies_sf_xray)
        state_machine_role_base.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_base.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_dominio = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_dominio")
        state_machine_role_dominio.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_dominio.add_to_policy(policies_sf_xray)
        state_machine_role_dominio.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_dominio.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_comercial = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_comercial")
        state_machine_role_comercial.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_comercial.add_to_policy(policies_sf_xray)
        state_machine_role_comercial.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_comercial.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_cadena = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_cadena")
        state_machine_role_cadena.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_cadena.add_to_policy(policies_sf_xray)
        state_machine_role_cadena.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_cadena.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_redshift = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_redshift")
        state_machine_role_redshift.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_redshift.add_to_policy(policies_sf_xray)
        state_machine_role_redshift.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_redshift.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_analytics = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_analytics")
        state_machine_role_analytics.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_analytics.add_to_policy(policies_sf_xray)
        state_machine_role_analytics.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_analytics.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_dominioeconored = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_dominio_econored")
        state_machine_role_dominioeconored.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_dominioeconored.add_to_policy(policies_sf_xray)
        state_machine_role_dominioeconored.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_dominioeconored.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_comercialeconored = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_comercial_econored")
        state_machine_role_comercialeconored.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_comercialeconored.add_to_policy(policies_sf_xray)
        state_machine_role_comercialeconored.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_comercialeconored.add_to_policy(policies_sf_start_state_machine)
        state_machine_role_analytics_econored = resource_creator.create_iam_role(SERVICES.STEP_FUNCTION, "state_machine_analytics_econored")
        state_machine_role_analytics_econored.add_to_policy(policies_sf_execute_jobs)
        state_machine_role_analytics_econored.add_to_policy(policies_sf_xray)
        state_machine_role_analytics_econored.add_to_policy(policies_sf_invoke_lambda)
        state_machine_role_analytics_econored.add_to_policy(policies_sf_start_state_machine)


        lambda_get_data_role = resource_creator.create_iam_role(SERVICES.LAMBDA_FUNCTION, "lambda_get_data")
        lambda_get_data_role.add_to_policy(policies_lambda_get_jobs)
        lambda_get_data_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        lambda_crawler_role = resource_creator.create_iam_role(SERVICES.LAMBDA_FUNCTION, "lambda_crawler")
        lambda_crawler_role.add_to_policy(policies_lambda_crawler)
        lambda_crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        ################################################
        #            GLUE CONNECTIONS                  #
        ################################################

        glue_redshift_catalog_connection, glue_redshift_catalog_connection_name = resource_creator.create_glue_connection(
            'glue_to_redshift',
            core.Aws.ACCOUNT_ID,
            props['GLUE_CONN_REDSHIFT_JDBC'],
            props['GLUE_CONN_REDSHIFT_PASS'],
            props['GLUE_CONN_REDSHIFT_USER'],
            props['GLUE_CONN_REDSHIFT_AVAILABILITY_ZONE'],
            props['GLUE_CONN_REDSHIFT_SG'],
            props['GLUE_CONN_REDSHIFT_SUBNET']
        )

        ################################################
        #            GLUE JOBS                         #
        ################################################
        jobs_args = {
            '--extra-py-files' : f's3://{artifatcs_bucket.bucket_name}/{PATH_UTILS.PATH_AWS_ARTIFCATS_LAYERS}common_jobs_functions.py',
            '--S3_PATH_STG': f"s3a://{stage_bucket.bucket_name}/athenea/",
            '--S3_PATH_ANALYTICS' : f"s3a://{analytics_bucket.bucket_name}/athenea/analytics/",
            '--S3_PATH_EXTERNAL' : f"s3a://{external_bucket.bucket_name}/aje/",
            '--S3_PATH_ARTIFACTS' : f"s3a://{artifatcs_bucket.bucket_name}/{PATH_UTILS.PATH_AWS_ARTIFACTS}",
            '--S3_PATH_ARTIFACTS_CSV': f"s3a://{artifatcs_bucket.bucket_name}/{PATH_UTILS.PATH_AWS_ARTIFACTS_CSV}",
            '--CATALOG_CONNECTION' : glue_redshift_catalog_connection_name,
            '--REGION_NAME' : 'us-east-2',
            '--DYNAMODB_DATABASE_NAME' : props['CREDENTIALS_DYNAMO_TABLE'] ,
            '--DYNAMODB_LOGS_TABLE' : props['LOGS_DYNAMO_TABLE'],
            '--COD_PAIS' : 'PE',
            '--INSTANCIAS': 'PE',
            '--COD_SUBPROCESO': '0',
            '--ERROR_TOPIC_ARN' : props['ERROR_TOPIC_ARN'],
            '--PROJECT_NAME' : 'athenea',
            '--datalake-formats' : "delta",
            '--enable-continuous-log-filter' : "true"
        }

        #Create Glue Jobs for models and targets
        cr_targets={}
        state_machine_order_list = {}
        relation_process = {}
        archivo_csv = './artifacts/code/csv/aje-jobs.csv'
        df = pd.read_csv(archivo_csv, delimiter = ';')
        for index, fila in df.iterrows():
            data = fila.to_dict()

            #will search scripts in PATH_LOCAL_GLUE_JOBS_ANALYTICS/models/{layer}/{procedure}.py
            #procedure = name of the job
            glue_csv_job,glue_csv_job_name = resource_creator.create_glue_job_from_bucket_script_analytics(
                data['layer'],
                data['procedure'],
                data['periods'],
                data['num_workers'],
                jobs_args,
                role = glue_role_access
            )
            

            if data['layer'] not in cr_targets:
                cr_targets[data['layer']] = []
            cr_targets[data['layer']].append(resource_creator.create_glue_delta_targets(analytics_bucket.bucket_name, f"athenea/analytics/{data['layer']}/{data['procedure']}/"))
            
            if data['exe_order'] > 0:
                #layer: -> order: -> [procedure]
                if data['layer'] not in state_machine_order_list:
                    state_machine_order_list[data['layer']] = {}
                if str(data['exe_order']) not in state_machine_order_list[data['layer']]:
                    state_machine_order_list[data['layer']][str(data['exe_order'])] = []
                state_machine_order_list[data['layer']][str(data['exe_order'])].append({'table': data['procedure'], 'process_id': str(data['process_id']), 'job_name':glue_csv_job_name})

                if str(data['process_id']) not in relation_process:
                    relation_process[str(data['process_id'])] = []
                relation_process[str(data['process_id'])].append({'table': data['procedure'], 'periods': data['periods'], 'layer': data['layer']})

        #add relation process to job args
        for key in relation_process:
            jobs_args[f'--P{key}'] = json.dumps(relation_process[key])
        jobs_args['--LOAD_PROCESS'] = '10'
        jobs_args['--ORIGIN'] = 'AJE'

        #Create Glue Job for load to redshift final datamodels
        load_to_redshift_job,load_to_redshift_job_name =  resource_creator.create_glue_job('load_to_redshift',
                                                            jobs_args,
                                                            f"{PATH_UTILS.PATH_AWS_ARTIFCATS_GLUE_JOBS_ANALYTICS}redshift/load_to_redshift.py",
                                                            glue_role_access,
                                                            connection_name = [glue_redshift_catalog_connection_name])

        #Create Glue Job for load to redshift stage data to big_magic_incoming schemas
        loadt_stage_to_redshift_job,loadt_stage_to_redshift_job_name =  resource_creator.create_glue_job('loadt_stage_to_redshift',
                                                            jobs_args,
                                                            f"{PATH_UTILS.PATH_AWS_ARTIFCATS_GLUE_JOBS_ANALYTICS}redshift/loadt_stage_to_redshift.py",
                                                            glue_role_access,
                                                            connection_name = [glue_redshift_catalog_connection_name])

        ################################################
        #               GLUE DATABASES                 #
        ################################################
        databases_layers = {}
        for layer_key in cr_targets:
            databases_obj, databases_name = resource_creator.create_glue_database(core.Aws.ACCOUNT_ID, layer_key)
            databases_layers[layer_key] = [databases_obj,databases_name]

        ################################################
        #            GLUE CRWALERS                     #
        ################################################
        crawlers_layers =  {}
        for layer_key in cr_targets:
            crawler_obj, crawler_name = resource_creator.create_glue_delta_crawler(
                layer_key,
                databases_layers[layer_key][1],
                role = crawler_role,
                targets = cr_targets[layer_key]
            )
            crawlers_layers[layer_key] = [crawler_obj,crawler_name]
        
        ################################################
        #            LAMBDA FUNCTIONS                  #
        ################################################

        #Create Lambda Functions
        lambda_get_data = resource_creator.create_lambda_function(
            name='get_data',
            lambda_filename=PATH_UTILS.PATH_LOCAL_ARTIFCATS_LAMBDA + 'get_data/',
            role=lambda_get_data_role,
            timeout_min=10
        )

        lambda_crawler = resource_creator.create_lambda_function(
            name='crawler',
            lambda_filename=PATH_UTILS.PATH_LOCAL_ARTIFCATS_LAMBDA + 'crawler/',
            role=lambda_crawler_role,
            timeout_min=10
        )

        ################################################
        #                STEP FUNCTIONS                #
        ################################################

        #Create State Machines

        definition_state_machine_base = resource_creator.create_state_machine_definition_base(
            lambda_function = lambda_get_data,
            sns_topic = sns_error_topic
        )

        state_machine_base = resource_creator.create_step_function(
            name='analytics_base',
            role=state_machine_role_base,
            definition=definition_state_machine_base
        )

        definition_state_machine_base_redfhist = resource_creator.create_state_machine_definition_base(
            lambda_function = lambda_get_data,
            sns_topic = sns_error_topic,
            redshift = True
        )

        state_machine_base_redshift = resource_creator.create_step_function(
            name='analytics_base redshift',
            role=state_machine_role_base,
            definition=definition_state_machine_base_redfhist
        )

        #Dominio state machine
        definition_state_machine_dominio = resource_creator.create_state_machine_definition_layer(
            lambda_crawler = lambda_crawler,
            layer_order = state_machine_order_list['dominio'],
            sns_topic = sns_error_topic,
            glue_crawler_name = crawlers_layers['dominio'][1],
            layer = 'dominio',
            state_machine_base_arn=state_machine_base.state_machine_arn
        )

        state_machine_dominio = resource_creator.create_step_function(
            name='analytics_dominio',
            role=state_machine_role_dominio,
            definition=definition_state_machine_dominio
        )

        #Comercial state machine
        definition_state_machine_comercial = resource_creator.create_state_machine_definition_layer(
            lambda_crawler = lambda_crawler,
            layer_order = state_machine_order_list['comercial'],
            sns_topic = sns_error_topic,
            glue_crawler_name = crawlers_layers['comercial'][1],
            layer = 'comercial',
            state_machine_base_arn=state_machine_base.state_machine_arn
        )

        state_machine_comercial = resource_creator.create_step_function(
            name='analytics_comercial',
            role=state_machine_role_comercial,
            definition=definition_state_machine_comercial
        )
        
        #Cadena state machine
        definition_state_machine_cadena = resource_creator.create_state_machine_definition_layer(
            lambda_crawler = lambda_crawler,
            layer_order = state_machine_order_list['cadena'],
            sns_topic = sns_error_topic,
            glue_crawler_name = crawlers_layers['cadena'][1],
            layer = 'cadena',
            state_machine_base_arn=state_machine_base.state_machine_arn
        )

        state_machine_cadena = resource_creator.create_step_function(
            name='analytics_cadena',
            role=state_machine_role_cadena,
            definition=definition_state_machine_cadena
        )

        #Redshift state machine
        definition_state_machine_redshift = resource_creator.create_state_machine_definition_redshift(
            glue_job_name = load_to_redshift_job_name,
            state_machine_base_arn=state_machine_base_redshift.state_machine_arn,
            origin = 'IN'
        )

        state_machine_redshift = resource_creator.create_step_function(
            name='analytics_redshift',
            role=state_machine_role_redshift,
            definition=definition_state_machine_redshift
        )
        
        #Analytics orchestrate
        definition_state_machine_analytics = resource_creator.create_state_machine_definition_analytics(
            dominio_arn = state_machine_dominio.state_machine_arn,
            comercial_arn = state_machine_comercial.state_machine_arn,
            cadena_arn = state_machine_cadena.state_machine_arn,
            redshift_arn = state_machine_redshift.state_machine_arn
        )

        state_machine_analytics = resource_creator.create_step_function(
            name='analytics_orchestrate',
            role=state_machine_role_analytics,
            definition=definition_state_machine_analytics
        )

############################
        #Dominio Econored state machine
        definition_state_machine_dominio_econored = resource_creator.create_state_machine_definition_layer(
            lambda_crawler = lambda_crawler,
            layer_order = state_machine_order_list['dominio_econored'],
            sns_topic = sns_error_topic,
            glue_crawler_name = crawlers_layers['dominio_econored'][1],
            layer = 'dominio_econored',
            state_machine_base_arn=state_machine_base.state_machine_arn
        )

        state_machine_dominio_econored = resource_creator.create_step_function(
            name='analytics_dominio_econored',
            role=state_machine_role_dominioeconored,
            definition=definition_state_machine_dominio_econored
        )

        #Comercial Econored state machine
        definition_state_machine_comercial_econored = resource_creator.create_state_machine_definition_layer(
            lambda_crawler = lambda_crawler,
            layer_order = state_machine_order_list['comercial_econored'],
            sns_topic = sns_error_topic,
            glue_crawler_name = crawlers_layers['comercial_econored'][1],
            layer = 'comercial_econored',
            state_machine_base_arn=state_machine_base.state_machine_arn
        )

        state_machine_comercial_econored = resource_creator.create_step_function(
            name='analytics_comercial_econored',
            role=state_machine_role_comercialeconored,
            definition=definition_state_machine_comercial_econored
        )

        #Redshift Econored state machine
        definition_state_machine_redshift_econored = resource_creator.create_state_machine_definition_redshift(
            glue_job_name = load_to_redshift_job_name,
            state_machine_base_arn=state_machine_base_redshift.state_machine_arn,
            origin = 'ECONORED'
        )
        state_machine_redshift_econored = resource_creator.create_step_function(
            name='analytics_redshift_econored',
            role=state_machine_role_redshift,
            definition=definition_state_machine_redshift_econored
        )

        #Analytics Econored Orchestrate
        definition_state_machine_analytics_econored = resource_creator.create_state_machine_definition_analytics_econored(
            dominio_econored_arn = state_machine_dominio_econored.state_machine_arn,
            comercial_econored_arn = state_machine_comercial_econored.state_machine_arn,
            redshift_econored_arn = state_machine_redshift_econored.state_machine_arn
        )

        state_machine_analytics_econored = resource_creator.create_step_function(
            name='analytics_orchestrate_econored',
            role=state_machine_role_analytics_econored,
            definition=definition_state_machine_analytics_econored
        )