

from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, DeploymentFilter
import asyncio
# import pandas as pd

async def get_data_status():
    async with get_client() as client:
            
        flow_runs = await client.read_flow_runs(
            limit=1,
            sort="START_TIME_DESC",
            deployment_filter=DeploymentFilter(name={'any_': ["dbt-analytics-master"]}),
            flow_run_filter=FlowRunFilter(state={"name": {"any_": ["Completed", "Failed", "Crashed", "TimedOut"]}})
        )

        # print(flow_runs[0])

        # states = []
        # for run in flow_runs:
            # print(f"Flow run {run.id} state: {run.state.name}, start time: {run.start_time}")
        #     states.append(run.state.name)
        # df = pd.DataFrame(states, columns=['state'])
        # print(df['state'].value_counts())
        if not flow_runs[0].state.is_completed():
            return False
        else:
            return True


if __name__ == "__main__":
    asyncio.run(get_data_status())