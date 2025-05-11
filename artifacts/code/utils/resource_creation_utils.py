import json
from artifacts.code.utils.resource_names_utils import NAMES_CREATOR, SERVICES, DMS_ENDPOINT_TYPE
from artifacts.code.utils.path_utils import PATH_UTILS
from aws_cdk import (
    Tags,
    Duration,
    SecretValue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_dynamodb as dynamodb,
    aws_dms as dms,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    aws_stepfunctions as sf,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_events as events
)

class RESOURCE_CREATOR():
    def __init__(self, stack, project_name, environment, enterprise, separator= '-'):
        self.stack = stack
        self.project_name = project_name
        self.environment = environment
        self.enterprise = enterprise
        self.separator = separator
        self.name_creator = NAMES_CREATOR(project_name.lower().replace(" ", "_"), environment, enterprise, separator)

    def set_artifacts_bucket(self, artifact_bucket):
        self.artifacts_bucket = artifact_bucket

    def tag_resource(self, resource, name, service_name):
        Tags.of(resource).add(f"Owner", "Favio Cuya")
        # Tags.of(resource).add(f"Owner_Modifiedby", self.default_owner)
        # Tags.of(resource).add(f"Name", name)
        # Tags.of(resource).add(f"Application_Name", self.project_name)
        # Tags.of(resource).add(f"Environment", self.environment.upper())
        # Tags.of(resource).add(f"Service_Name",  service_name)
        # Tags.of(resource).add(f"Cost_Center",  '0')
        # Tags.of(resource).add(f"Project_Code",  self.project_id)
        # Tags.of(resource).add(f"Tech_Owner",  self.tech_owner)
        # Tags.of(resource).add(f"Business_Unit", self.business_unit.upper().replace("-","_"))

    def import_bucket_from_name(self, bucket_name):
        bucket_name_internal = self.name_creator.create_resource_name(SERVICES.S3_BUCKET, bucket_name)
        bucket = s3.Bucket.from_bucket_name(self.stack, bucket_name_internal, bucket_name)
        return bucket

    def import_sns_topic_from_arn(self, topic_arn):
        topic_name = topic_arn.split(':')[-1]
        topic_name_internal = self.name_creator.create_resource_name(SERVICES.SNS_TOPIC, topic_name)
        topic = sns.Topic.from_topic_arn(self.stack, topic_name_internal, topic_arn)

        return topic

    def import_dynamodb_table_from_name(self, table_name):
        table_name_internal = self.name_creator.create_resource_name(SERVICES.DYNAMO_TABLE, table_name)
        table = dynamodb.Table.from_table_name(self.stack, table_name_internal, table_name)
        return table

    def create_iam_policy_statement(self, actions = [], resources = []):
        policy = iam.PolicyStatement(
            actions=actions,
            resources=resources
        )
        
        return policy

    def create_iam_policy(self, name, actions = [], resources = []):
        policy_name = self.name_creator.create_resource_name(SERVICES.IAM_POLICY, name)
        policy_statement = iam.PolicyStatement(actions = actions,resources=resources)
        policy = iam.Policy( 
            self.stack,
            policy_name,
            policy_name=policy_name
        )
        policy.add_statements(policy_statement)
        self.tag_resource(policy, policy_name, "AWS IAM")

        return policy

    def create_glue_job(self, name, environment, code_path, role : iam.Role, timeout = 1, connection_name = []):
        connection_glue = []
        if connection_name != []:
            for conn_name in connection_name:
                conn = glue_alpha.Connection.from_connection_name(self.stack, f"{name}-{conn_name}", conn_name)
                connection_glue.append(conn)

        glue_job_name = self.name_creator.create_resource_name(SERVICES.GLUE_JOB, name)
        environment['--PROCESS_NAME'] = glue_job_name
        glue_job = glue_alpha.Job(self.stack, glue_job_name,
            job_name=glue_job_name,
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(self.artifacts_bucket,code_path)
            ),
            connections=connection_glue,
            default_arguments=environment,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            enable_profiling_metrics=True,
            timeout=Duration.hours(timeout),
            max_concurrent_runs=100,
            role=role
            )

        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=[
                    f"arn:aws:logs:*:*:log-group:/aws/*:*",
                    "arn:aws:logs:*:*:log-group:/aws-glue/*"
                ]
            )
        )
        self.tag_resource(glue_job, glue_job_name, "AWS Glue")

        return glue_job, glue_job_name
    
    def create_glue_connection(self, name, catalog_id, connection_jdbc_url, connection_password, connection_username, connection_az,connection_security_group_id , connection_subnet_id):
        glue_connection_name = self.name_creator.create_resource_name(SERVICES.GLUE_CONNECTION, name)
        glue_connection = glue.CfnConnection(self.stack, glue_connection_name,
            catalog_id=catalog_id,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name=glue_connection_name,
                connection_properties={
                    "JDBC_CONNECTION_URL": connection_jdbc_url,
                    "PASSWORD": connection_password,
                    "USERNAME": connection_username,
                },
                connection_type="JDBC",
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    availability_zone=connection_az,
                    security_group_id_list=[connection_security_group_id],
                    subnet_id=connection_subnet_id,
                )
            )
        )
        self.tag_resource(glue_connection, glue_connection_name, "AWS Glue")
        return glue_connection, glue_connection_name
    
    def create_glue_job_from_bucket_script_analytics(self, layer,  name, periods,n_workers, environment, role : iam.Role, timeout = 1):
        
        #specific parameters
        if name == 'fact_movimiento_inventario_diario':
            environment['--USE_HARDCODED_DATE'] = 'yyyy-mm-dd'

        glue_job_name = self.name_creator.create_resource_name(SERVICES.GLUE_JOB, f"{layer}-{name}")
        environment['--PROCESS_NAME'] = glue_job_name
        environment['--FLOW_NAME'] = layer
        environment['--PERIODS'] = str(periods)
        glue_job = glue_alpha.Job(self.stack, glue_job_name,
            job_name=glue_job_name,
            executable=glue_alpha.JobExecutable.python_etl(
                    glue_version=glue_alpha.GlueVersion.V4_0,
                    python_version=glue_alpha.PythonVersion.THREE,
                    script=glue_alpha.Code.from_bucket(
                        self.artifacts_bucket, f"{PATH_UTILS.PATH_AWS_ARTIFCATS_GLUE_JOBS_ANALYTICS}models/{layer}/{name}.py"
                    )
            ),
            default_arguments=environment,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=n_workers,
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            enable_profiling_metrics=True,
            timeout=Duration.hours(timeout),
            max_concurrent_runs=100,
            role=role
            )

        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=[
                    f"arn:aws:logs:*:*:log-group:/aws/*:*",
                    "arn:aws:logs:*:*:log-group:/aws-glue/*"
                ]
            )
        )
        self.tag_resource(glue_job, glue_job_name, "AWS Glue")

        return glue_job, glue_job_name

    def create_glue_database(self,catalog_id, name):
        glue_database_name = self.name_creator.create_resource_name(SERVICES.GLUE_DATABASE, name)
        glue_database = glue.CfnDatabase(self.stack, glue_database_name,
            catalog_id=catalog_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=glue_database_name
            )
        )
        
        self.tag_resource(glue_database, glue_database_name, "AWS Glue")
        return glue_database, glue_database_name

    def create_glue_delta_targets(self, s3_bucket, s3_prefix):
        glue_delta_target = glue.CfnCrawler.DeltaTargetProperty(
                create_native_delta_table=False,
                delta_tables=[
                    f"s3://{s3_bucket}/{s3_prefix}"
                ],
                write_manifest=True,
            )
        

        return glue_delta_target

    def create_glue_delta_crawler(self, name, database_name, role : iam.Role, targets):
        glue_crawler_name = self.name_creator.create_resource_name(SERVICES.GLUE_CRAWLER, name)
        glue_crawler = glue.CfnCrawler(self.stack, glue_crawler_name,
            name=glue_crawler_name,
            database_name=database_name,
            role=role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(delta_targets=targets),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE"
            )
        )
        self.tag_resource(glue_crawler, glue_crawler_name, "AWS Glue")

        return glue_crawler, glue_crawler_name
            
    def create_step_function(self, name, role,definition):
        step_function_name = self.name_creator.create_resource_name(SERVICES.STEP_FUNCTION, name)
        step_function = sf.StateMachine(self.stack, step_function_name,
            state_machine_name=step_function_name,
            role=role,
            definition=definition
        )
        self.tag_resource(step_function, step_function_name, "AWS Step Functions")
        return step_function
    
    def create_task_lambda_invoke(self, name, lambda_function, result_path):
        task = tasks.LambdaInvoke(self.stack,
            name,
            lambda_function=lambda_function,
            output_path=result_path
        )

        return task
    
    def create_task_wait(self, name, time):
        task = sf.Wait(self.stack,
            name,
            time=sf.WaitTime.duration(Duration.seconds(time))
        )

        return task
    
    def create_task_choice(self, name, choices, default_choice):
        task = sf.Choice(self.stack,
            name
        )

        for choice in choices:
            task.when(choice['condition'], choice['next'])

        task.otherwise(default_choice)

        return task
    
    def create_task_glue_start_job_run(self, name, arguments: sf.TaskInput, glue_job_name = ''):
        task = tasks.GlueStartJobRun(self.stack,
            name,
            glue_job_name=glue_job_name,
            arguments=arguments,
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        return task
    
    def create_task_pass(self, name):
        task = sf.Pass(self.stack,
            name
        )

        return task
    
    def create_task_sns_publish(self, name, topic, message: sf.TaskInput):
        task = tasks.SnsPublish(self.stack,
            name,
            topic=topic,
            message=message
        )

        return task

    def create_sns_topic(self, name):
        sns_topic_name = self.name_creator.create_resource_name(SERVICES.SNS_TOPIC, name)
        sns_topic = sns.Topic(self.stack, sns_topic_name,
            topic_name=sns_topic_name
        )
        self.tag_resource(sns_topic, sns_topic_name, "AWS SNS")
        return sns_topic

    def create_state_machine_definition_base(self, lambda_function, sns_topic,redshift = False):
        if redshift:
            redshift_name = 'redshift'
        else:
            redshift_name = ''
        # Step 1: Create Independent Tasks
        # Task: envio de errores interno (Pass State)
        envio_de_errores_interno = self.create_task_pass(name=f"envio de errores interno {redshift_name}")

        # Task: saltar ejecución (Pass State)
        saltar_ejecucion = self.create_task_pass(name=f"saltar ejecución {redshift_name}")

        # Task: envío de errores lambda (SNS Publish Task)
        envio_de_errores_lambda = self.create_task_sns_publish(
            name=f"envío de errores lambda {redshift_name}",
            topic=sns_topic,
            message=sf.TaskInput.from_object({
                "State machine": "datalake-aje-prd-athenea-base-sm",
                "Lambda": "datalake-aje-prd-athenea-get_data-lambda",
                "Error": sf.JsonPath.string_at("$.msg")
            })
        )
        envio_de_errores_interno_sns = self.create_task_sns_publish(
            name=f"envio de errores interno sns {redshift_name}",
            topic=sns_topic,
            message=sf.TaskInput.from_object({
                "State machine": "datalake-aje-prd-athenea-base-sm",
                "Job name": sf.JsonPath.string_at("$.job_name"),
                "Error": sf.JsonPath.string_at("$.output.Error"),
                "Cause": sf.JsonPath.string_at("$.output.Cause")
            })
        )

        envio_de_errores_interno.next(envio_de_errores_interno_sns)

        # Step 2: Create Dependent Tasks
        # Task: Inicia Glue job

        if not redshift:
            base_arguments = sf.TaskInput.from_object({
                    "--COD_PAIS": sf.JsonPath.string_at("$.COD_PAIS"),
                    "--INSTANCIAS": sf.JsonPath.string_at("$.INSTANCIAS"),
                })
        else:
            base_arguments = sf.TaskInput.from_object({
                    "--COD_PAIS": sf.JsonPath.string_at("$.COD_PAIS"),
                    "--LOAD_PROCESS": sf.JsonPath.string_at("$.exe_process_id"),
                    "--ORIGIN": sf.JsonPath.string_at("$.ORIGIN"),
                    "--INSTANCIAS": sf.JsonPath.string_at("$.INSTANCIAS"),
                })

        inicia_glue_job = self.create_task_glue_start_job_run(
            name=f"Inicia Glue job {redshift_name}",
            glue_job_name=sf.JsonPath.string_at("$.job_name"),  # Reference the input variable job_name
            arguments=base_arguments
        )
        inicia_glue_job.add_catch(
            envio_de_errores_interno,
            errors=["States.ALL"],
            result_path="$.output"
        )

        # Step 3: Create Choice State (Choice)
        choice_state = sf.Choice(
            self.stack,
            f"Choice {redshift_name}"
        ).when(
            sf.Condition.boolean_equals("$.ready", True),
            inicia_glue_job
        ).when(
            sf.Condition.boolean_equals("$.error", True),
            envio_de_errores_lambda
        ).otherwise(
            saltar_ejecucion
        )

        # Step 4: Create Wait Task
        wait_task = self.create_task_wait(
            name=f"Wait {redshift_name}",
            time=60
        )

        # Step 5: Create Valida ejecución concurrente (Choice State)
        valida_ejecucion_concurrente = sf.Choice(
            self.stack,
            f"Valida ejecución concurrente {redshift_name}"
        ).when(
            sf.Condition.boolean_equals("$.wait", True),
            wait_task
        ).otherwise(
            choice_state  # Default transition to Choice
        )

        # Step 6: Create Lambda Base Task
        lambda_task = self.create_task_lambda_invoke(
            name=f"Lambda Base {redshift_name}",
            lambda_function=lambda_function,
            result_path="$.Payload"
        )
        lambda_task.add_retry(
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException"
            ],
            interval=Duration.seconds(1),
            max_attempts=3,
            backoff_rate=2.0
        )
        wait_task.next(lambda_task) 
        lambda_task.next(valida_ejecucion_concurrente)

        # Step 7: Return the State Machine Definition
        return lambda_task

    def create_state_machine_definition_layer(self, lambda_crawler, glue_crawler_name, sns_topic, state_machine_base_arn, layer_order: dict, layer: str):
        # Step 1: Initialize Variables
        current_state = None  # This will hold the last created state
        initial_state = None  # This will hold the initial state
        order = 1  # Start with the first order

        # Step 2: Iterate Through Layer Orders
        while str(order) in layer_order:
            # Create a Parallel State for the Current Order
            parallel_state = sf.Parallel(
                self.stack,
                f"{layer} {order}",
                result_path="$.execute"
            )

            # Add Glue Jobs to the Parallel State
            for table in layer_order[str(order)]:
                table_name = f"{table['table']}"
                #layers subnames
                if "dominio" in layer:
                    table_name = table_name + "_dom"
                elif "comercial" in layer:
                    table_name = table_name + "_com"
                elif "cadena" in layer:
                    table_name = table_name + "_cad"
                #origins subnames
                if "econored" in layer:
                    table_name = table_name + "_3c"
                #end of name
                table_name = table_name + "-base-sm"
                glue_job_task = tasks.StepFunctionsStartExecution(
                    self.stack,
                    f"Execute {layer} {table['table']}",
                    state_machine=sf.StateMachine.from_state_machine_arn(
                        self.stack,
                        table_name,
                        state_machine_base_arn
                    ),
                    input=sf.TaskInput.from_object({
                        "exe_process_id.$": "$.exe_process_id",
                        "INSTANCIAS.$": "$.INSTANCIAS",
                        "COD_PAIS.$": "$.COD_PAIS",
                        "table": table['table'],
                        "own_process_id": table['process_id'],
                        "job_name": table['job_name'],
                        "redshift": False,
                    }),
                    integration_pattern=sf.IntegrationPattern.RUN_JOB
                )
                parallel_state.branch(glue_job_task)

            # Chain the Parallel State
            if current_state is None:
                initial_state = parallel_state
            if current_state:
                current_state.next(parallel_state)
                
            current_state = parallel_state

            # Move to the Next Order
            order += 1

        # Step 3: Add Lambda Crawler Task
        lambda_crawler_task = tasks.LambdaInvoke(
            self.stack,
            f"Crawlers lambda {layer}",
            lambda_function=lambda_crawler,
            payload=sf.TaskInput.from_object({
                "CRAWLER_NAME": glue_crawler_name
            }),
            output_path="$.Payload"
        )
        lambda_crawler_task.add_retry(
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException"
            ],
            interval=Duration.seconds(1),
            max_attempts=3,
            backoff_rate=2.0
        )

        # Chain the Lambda Crawler Task
        current_state.next(lambda_crawler_task)

        wait_task = self.create_task_wait(
            name=f"Wait {layer}",
            time=60
        )

        # Step 4: Add Choice State for Concurrent Execution Validation
        valida_ejecucion_concurrente = sf.Choice(
            self.stack,
            f"Valida ejecución concurrente {layer}"
        ).when(
            sf.Condition.boolean_equals("$.wait", True),
            wait_task.next(lambda_crawler_task)
        ).when(
            sf.Condition.boolean_equals("$.error", True),
            self.create_task_sns_publish(
                name=f"envio de errores lambda crawlers {layer}",
                topic=sns_topic,
                message=sf.TaskInput.from_object({
                    "State machine": "datalake-aje-prd-athenea-dominio-comercial-sm",
                    "Lambda": "datalake-aje-prd-athenea-crawlers-lambda",
                    "Error.$": "$.msg"
                })
            )
        ).when(
            sf.Condition.boolean_equals("$.wait", False),
            tasks.CallAwsService(
                self.stack,
                f"Inicia Crawler {layer}",
                service="glue",
                action="startCrawler",
                parameters={"Name": glue_crawler_name},
                iam_resources=["*"],
                result_path="$.crawlerResult"
            ).add_catch(
                self.create_task_sns_publish(
                    name=f"envio de errores ejecución crawler {layer}",
                    topic=sns_topic,
                    message=sf.TaskInput.from_object({
                        "State machine": "datalake-aje-prd-athenea-dominio-comercial-sm",
                        "Crawler": glue_crawler_name,
                        "Error": "Fallo al ejecutar Crawler de dominio"
                    })
                ),
                errors=["States.ALL"]
            )
        )

        lambda_crawler_task.next(valida_ejecucion_concurrente)

        # Step 5: Return the State Machine Definition
        return initial_state

    def create_state_machine_definition_redshift(self, glue_job_name, state_machine_base_arn, origin):
        # Call state machine base
        glue_job_task = tasks.StepFunctionsStartExecution(
            self.stack,
            f"Execute Glue Job {origin}",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                f"Start load to redshift {origin}",
                state_machine_base_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS",
                "ORIGIN.$": "$.ORIGIN",
                "table": "load_to_redshift",
                "own_process_id": '-1',
                "job_name": glue_job_name,
                "redshift": True,
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        
        return glue_job_task

    def create_state_machine_definition_analytics(self, dominio_arn, comercial_arn, cadena_arn, redshift_arn):
        
        #1.1 dominio - 2.1 comercial - 2.2 cadena - 3.1 redshift
        # start invoking dominio
        # wait for dominio
        # Next in parallel start comercial and cadena
        # wait for comercial and cadena
        # Next start redshift
        # wait for redshift
        # end

        # Start State Machine
        dominio_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Dominio State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Dominio",
                dominio_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        # Next in parallel start comercial and cadena
        parallel_state = sf.Parallel(
            self.stack,
            "Parallel Comercial and Cadena",
            result_path="$.execute"
        )

        # Start Comercial State Machine
        comercial_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Comecial State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Comercial",
                comercial_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        parallel_state.branch(comercial_state_machine)

        # Start Cadena State Machine
        cadena_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Cadena State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Cadena",
                cadena_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        parallel_state.branch(cadena_state_machine)

        # Next start redshift
        redshift_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Redshift State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Redshift",
                redshift_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS",
                "redshift": True,
                "own_process_id": "redshift",
                "ORIGIN": "IN"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        
        dominio_state_machine.next(parallel_state)
        parallel_state.next(redshift_state_machine)

        return dominio_state_machine

    def create_state_machine_definition_analytics_econored(self, dominio_econored_arn, comercial_econored_arn, redshift_econored_arn):
        
        #1.1 dominio - 2.1 comercial - 3.1 redshift
        # start invoking dominio
        # wait for dominio
        # Next in parallel start comercial
        # wait for comercial
        # Next start redshift
        # wait for redshift
        # end

        # Start State Machine
        dominio_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Dominio Econored State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Dominio Econored",
                dominio_econored_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )

        # Next in parallel start comercial and cadena
        parallel_state = sf.Parallel(
            self.stack,
            "Parallel Comercial Econored",
            result_path="$.execute"
        )

        # Start Comercial State Machine
        comercial_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Comecial Econored State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Comercial Econored",
                comercial_econored_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        parallel_state.branch(comercial_state_machine)

        # Next start redshift
        redshift_state_machine = tasks.StepFunctionsStartExecution(
            self.stack,
            "Execute Redshift Econored State Machine",
            state_machine=sf.StateMachine.from_state_machine_arn(
                self.stack,
                "Redshift Econored",
                redshift_econored_arn
            ),
            input=sf.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "COD_PAIS.$": "$.COD_PAIS",
                "redshift": True,
                "own_process_id": "redshift",
                "ORIGIN": "ECONORED"
            }),
            result_path="$.execute",
            integration_pattern=sf.IntegrationPattern.RUN_JOB
        )
        
        dominio_state_machine.next(parallel_state)
        parallel_state.next(redshift_state_machine)

        return dominio_state_machine

    def create_iam_role(self, service : SERVICES, name : str):
        role_name = self.name_creator.create_resource_name(SERVICES.IAM_ROLE, name)
        service_principals = {
            SERVICES.S3_BUCKET : "s3.amazonaws.com",
            SERVICES.LAMBDA_FUNCTION : "lambda.amazonaws.com",
            SERVICES.DMS_INSTANCE_US_EAST_1 : "dms.us-east-1.amazonaws.com",
            SERVICES.DMS_INSTANCE : "dms.amazonaws.com",
            SERVICES.GLUE_JOB : "glue.amazonaws.com",
            SERVICES.STEP_FUNCTION : "states.amazonaws.com",
        }
        
        role = iam.Role(self.stack, role_name,
            role_name=role_name,
            assumed_by=iam.ServicePrincipal(service_principals[service])
        )

        self.tag_resource(role, role_name, "AWS IAM")
        return role

    def create_dms_instance(self, name, project_security_group, replication_instance_class, subnet_group, engine_version):
        replication_instance_name = self.name_creator.create_resource_name(SERVICES.DMS_INSTANCE, name)
        replication_instance = dms.CfnReplicationInstance(self.stack, 
            replication_instance_name,
            replication_instance_identifier=replication_instance_name,  
            replication_instance_class=replication_instance_class,
            allocated_storage=150,
            vpc_security_group_ids=[project_security_group.security_group_id],
            replication_subnet_group_identifier=subnet_group, 
            publicly_accessible=False, 
            engine_version=engine_version
        )
        self.tag_resource(replication_instance, replication_instance_name, "AWS DMS")
        return replication_instance

    def create_dynamo_table(self, name : str, primary_key_name :str, sort_key_name : str = None):
        dynamo_table_name = self.name_creator.create_resource_name(SERVICES.DYNAMO_TABLE, name)
        if sort_key_name:
            sort_key_name = dynamodb.Attribute(
                name=sort_key_name,
                type=dynamodb.AttributeType.STRING
            )

        dynamo_table = dynamodb.Table(
            self.stack,
            dynamo_table_name,
            table_name = dynamo_table_name,
            partition_key=dynamodb.Attribute(
                name=primary_key_name,
                type=dynamodb.AttributeType.STRING
            ),
            sort_key= sort_key_name,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        self.tag_resource(dynamo_table, dynamo_table_name, "AWS DynamoDB")
        return dynamo_table

    def create_dms_replication_task(self, descriptive_name, load_type, replication_instance ,source_endpoint, target_endpoint, task_definition_location):
        
        with open(task_definition_location, 'r') as file:
            table_mappings = json.load(file) 

        replication_task_settings = '{"Logging": {"EnableLogging": true,"LogComponents": [{"Id": "SOURCE_UNLOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "SOURCE_CAPTURE","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_LOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_APPLY","Severity": "LOGGER_SEVERITY_INFO"},{"Id": "TASK_MANAGER","Severity": "LOGGER_SEVERITY_DEBUG"}]},"FullLoadSettings": { "TargetTablePrepMode": "DROP_AND_CREATE","CreatePkAfterFullLoad": false, "StopTaskCachedChangesApplied": false, "StopTaskCachedChangesNotApplied": false, "MaxFullLoadSubTasks": 4 , "TransactionConsistencyTimeout": 600, "CommitRate": 10000}}'
    
        replication_task_name = self.name_creator.create_resource_name(SERVICES.DMS_TASK, descriptive_name, 's3', load_type)
        replication_task = dms.CfnReplicationTask(self.stack, replication_task_name,
            replication_task_identifier=replication_task_name,
            migration_type=load_type,  
            replication_instance_arn=replication_instance.ref,
            source_endpoint_arn=source_endpoint.ref,
            table_mappings=json.dumps(table_mappings),  
            target_endpoint_arn=target_endpoint.ref,
            replication_task_settings=replication_task_settings
        )
        self.tag_resource(replication_task, replication_task_name, "AWS DMS")
        
        return replication_task
    
    def create_dms_replication_task_existing_endpoint(self, descriptive_name, load_type, replication_instance ,existing_arn, target_endpoint, task_definition_location):
        
        with open(task_definition_location, 'r') as file:
            table_mappings = json.load(file) 

        replication_task_settings = '{"Logging": {"EnableLogging": true,"LogComponents": [{"Id": "SOURCE_UNLOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "SOURCE_CAPTURE","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_LOAD","Severity": "LOGGER_SEVERITY_DEFAULT"},{"Id": "TARGET_APPLY","Severity": "LOGGER_SEVERITY_INFO"},{"Id": "TASK_MANAGER","Severity": "LOGGER_SEVERITY_DEBUG"}]},"FullLoadSettings": { "TargetTablePrepMode": "DROP_AND_CREATE","CreatePkAfterFullLoad": false, "StopTaskCachedChangesApplied": false, "StopTaskCachedChangesNotApplied": false, "MaxFullLoadSubTasks": 4 , "TransactionConsistencyTimeout": 600, "CommitRate": 10000}}'
    
        replication_task_name = self.name_creator.create_resource_name(SERVICES.DMS_TASK, descriptive_name, 's3', load_type)
        replication_task = dms.CfnReplicationTask(self.stack, replication_task_name,
            replication_task_identifier=replication_task_name,
            migration_type=load_type,  
            replication_instance_arn=replication_instance.ref,
            source_endpoint_arn=existing_arn,
            table_mappings=json.dumps(table_mappings),  
            target_endpoint_arn=target_endpoint.ref,
            replication_task_settings=replication_task_settings
        )
        self.tag_resource(replication_task, replication_task_name, "AWS DMS")
        
        return replication_task

    def create_dms_target_endpoint(self, descriptive_name, target_bucket, s3_access_role):
        target_endpoint_name = self.name_creator.create_dms_endpoint_name(descriptive_name, 's3', DMS_ENDPOINT_TYPE.TARGET)
        s3_settings = dms.CfnEndpoint.S3SettingsProperty(
            bucket_name=target_bucket.bucket_name,
            bucket_folder=f"dms/temp/{descriptive_name}",
            service_access_role_arn=s3_access_role.role_arn,
            add_column_name=True,
            data_format='parquet'
        )

        target_endpoint = dms.CfnEndpoint(self.stack,target_endpoint_name,
            endpoint_identifier = target_endpoint_name,
            endpoint_type="target",
            engine_name="s3",
            s3_settings=s3_settings
            )
        self.tag_resource(target_endpoint, target_endpoint_name, "AWS DMS")

        return target_endpoint

    def create_dms_source_endpoint(self, engine, secrets_origin, database_name, descriptive_name):
        source_endpoint_name = self.name_creator.create_dms_endpoint_name(descriptive_name, engine, DMS_ENDPOINT_TYPE.SOURCE)
        source_endpoint = dms.CfnEndpoint(self.stack, source_endpoint_name,
            endpoint_identifier = source_endpoint_name,
            endpoint_type="source",
            engine_name=engine,
            database_name=database_name,
            microsoft_sql_server_settings=secrets_origin
            )

        self.tag_resource(source_endpoint, source_endpoint_name, "AWS DMS")

        return source_endpoint

    def create_s3_bucket(self, bucket_type: str):
        bucket_name = self.name_creator.create_resource_name(SERVICES.S3_BUCKET, bucket_type)
        bucket = s3.Bucket(
            self.stack, 
            id=bucket_name,
            bucket_name=bucket_name,  
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        self.tag_resource(bucket, bucket_name, "AWS S3")
        return bucket
        
    def create_secret_database_credentials(self, secret_name: str, engine : str = "sqlserver" ):
        secret_name = self.name_creator.create_resource_name(SERVICES.SECRETS_MANAGER_SECRET, secret_name, descriptive_name_2=engine)
        secret_value = json.dumps({
                "username": "your_db_username",
                "password": "your_db_password",  
                "engine": "mysql",
                "host": "your_db_host",
                "port": "3306",
                "dbname": "your_db_name"
            })

        secret = secretsmanager.Secret(self.stack, secret_name,
            secret_name=secret_name,
            secret_string_value=SecretValue.unsafe_plain_text(secret_value)
        )

        self.tag_resource(secret, secret_name, "AWS Secrets Manager")
        return secret
    
    def create_lambda_layer(self, name: str, description: str, path_to_layer: str):
        layer_name = self.name_creator.create_resource_name(SERVICES.LAMBDA_LAYER, descriptive_name_1=name)
        lambda_layer = _lambda.LayerVersion(self.stack, layer_name,
            code=_lambda.Code.from_asset(f"{path_to_layer}"),
            description=description,
            layer_version_name = layer_name
        )
        
        self.tag_resource(lambda_layer, layer_name, "AWS Lambda")
        return lambda_layer

    def create_lambda_function(self, name: str, lambda_filename: str, role, timeout_min=5, environment: dict = dict({}), layers: list = []):
        lambda_name = self.name_creator.create_resource_name(SERVICES.LAMBDA_FUNCTION, name)
        
        lambda_function = _lambda.Function(self.stack, lambda_name,
            function_name=lambda_name,
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler=lambda_filename.split("/")[-2] +'.lambda_handler',
            code=_lambda.Code.from_asset(lambda_filename),
            role = role,
            environment=environment,
            layers=layers,
            timeout=Duration.minutes(timeout_min)
            )

        lambda_function.role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=[f"arn:aws:logs:*:*:log-group:/aws/lambda/{lambda_name}:*"]
            )
        )

        self.tag_resource(lambda_function, lambda_name, "AWS Lambda")
        return lambda_function

    def create_event_bridge_rule(self, name, hours_rate='5/2'):

        event_bridge_name = self.name_creator.create_resource_name(SERVICES.EVENT_BRIDGE, name)
        event_bridge = events.Rule(self.stack, event_bridge_name,
            rule_name = event_bridge_name,
            description="this event Bridge rule start the default load every 2 hours",
            schedule = events.Schedule.cron(
                minute='0',
                hour=hours_rate,  # cada 2 horas por defecto
                month='*',
                year='*',
                week_day='MON-FRI'  # De lunes a viernes
            )
        )

        self.tag_resource(event_bridge, event_bridge_name, "AWS Event Bridge")
        return event_bridge
    
    def create_event_bridge_rule_periodicity(self, name, periodicity_daily = '*'):

        event_bridge_name = self.name_creator.create_resource_name(SERVICES.EVENT_BRIDGE, name)
        event_bridge = events.Rule(self.stack, event_bridge_name,
            rule_name = event_bridge_name,
            description="this event Bridge rule start the default load every 2 hours",
            schedule = events.Schedule.cron(
                minute='0',
                hour='5',  
                month='*',
                year='*',
                day= periodicity_daily # De lunes a viernes
            )
        )

        self.tag_resource(event_bridge, event_bridge_name, "AWS Event Bridge")
        return event_bridge
