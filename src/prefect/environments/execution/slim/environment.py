import uuid
from typing import Any

import cloudpickle

import prefect
from prefect.environments.execution import Environment
from prefect.environments.storage import Docker
from prefect.utilities import logging


class SlimEnvironment(Environment):
    """
    SlimEnvironment is an environment built for quick deployment of flows without any
    large resource requirements.

    *Note*: This environment is not currently customizable. This may be subject to change.
    """

    def __init__(self) -> None:
        self.identifier_label = str(uuid.uuid4())
        self.logger = logging.get_logger("SlimEnvironment")

    def execute(  # type: ignore
        self, storage: "Docker", flow_location: str, **kwargs: Any
    ) -> None:
        """
        Execute flow using a local Dask executor.

        Args:
            - storage (Docker): the Docker storage object that contains information relating
                to the image which houses the flow
            - flow_location (str): the location of the Flow to execute
            - **kwargs (Any): additional keyword arguments to pass to the runner

        Raises:
            - TypeError: if the storage is not `Docker`
        """
        if not isinstance(storage, Docker):
            raise TypeError("SlimEnvironment requires a Docker storage option")

        try:
            from prefect.engine import get_default_flow_runner_class

            # Load serialized flow from file and run it with the default executor class
            with open(
                prefect.context.get(
                    "flow_file_path", "/root/.prefect/flow_env.prefect"
                ),
                "rb",
            ) as f:
                flow = cloudpickle.load(f)

                executor = prefect.engine.get_default_executor_class()()
                runner_cls = get_default_flow_runner_class()
                runner_cls(flow=flow).run(executor=executor)
        except Exception as exc:
            self.logger.error("Unexpected error raised during flow run: {}".format(exc))
            raise exc
