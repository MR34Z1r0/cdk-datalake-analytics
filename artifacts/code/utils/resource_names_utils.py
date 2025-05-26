from artifacts.code.utils.resource_list_utils import SERVICES, DMS_ENDPOINT_TYPE
import re

class NAMES_CREATOR():

    def __init__(self, project_name, environment, enterprise, separator= '-'):
        self.project_name = project_name
        self.environment = environment
        self.separator = separator
        self.enterprise = enterprise
        
    def create_resource_name(self, service, descriptive_name):
        resource_name = False
        separator = '-'

        if service == SERVICES.S3_BUCKET:
            aws_service_name = 's3'
        elif service == SERVICES.LAMBDA_FUNCTION:
            aws_service_name = 'fn'
        elif service == SERVICES.STEP_FUNCTION:
            aws_service_name = 'sm'
        elif service == SERVICES.IAM_ROLE:
            aws_service_name = 'role'
        elif service == SERVICES.IAM_POLICY:
            aws_service_name = 'policy'
        elif service == SERVICES.GLUE_JOB:
            aws_service_name = 'job'
        elif service == SERVICES.GLUE_CONNECTION:
            aws_service_name = 'cnx'
        elif service == SERVICES.GLUE_CRAWLER:
            aws_service_name = 'cw'
        elif service == SERVICES.GLUE_DATABASE:
            #aws_service_name = 'db'
            service_name = separator.join(["athenea", descriptive_name, "analytics", "db"])
            resource_name = True
        elif service == SERVICES.SNS_TOPIC:
            aws_service_name = 'sns'
        else:
            aws_service_name = 'service'

        if not resource_name:
            service_name = separator.join([self.project_name, self.enterprise, self.environment, descriptive_name, aws_service_name])
        
        return re.sub(r"[., ]", "-", service_name)

