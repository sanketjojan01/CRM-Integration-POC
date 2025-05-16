from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from Activities import ETLActivities



@workflow.defn
class ETLWorkflow: 
    @workflow.run
    async def run(self, input_file_path) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2)
            # non_retryable_error_types=[]
        )
        extract_dict_output = await workflow.execute_activity_method(
            ETLActivities.transformCSVToDict,
            input_file_path,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=retry_policy   
        )

        load_flag = await workflow.execute_activity_method(
            ETLActivities.transformDictToJSON,
            extract_dict_output,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=retry_policy 
        )

        notify_flag = await workflow.execute_activity_method(
            ETLActivities.notifySQS,
            load_flag,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=retry_policy 
        )

        
