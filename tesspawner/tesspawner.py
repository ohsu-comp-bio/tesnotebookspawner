"""
A Spawner for JupyterHub that runs each user's server in a separate docker container spun up by TES
"""

import attr
import json
import requests

from tornado import gen
from jupyterhub.spawner import Spawner
from traitlets import (
    Dict,
    Unicode,
    Bool,
    Int,
    Any,
    default
)

from .tes import (
    Task,
    TaskParameter,
    Volume,
    Resources,
    DockerExecutor,
    clean_task_message
)


class TesSpawner(Spawner):
    _endpoint = "http://127.0.0.1:6000/v1/jobs"
    _jobID = ""

    def _create_message(self):
        message = Task(
            name="jupyterhub-singleusernotbook",
            projectID=None,
            description=None,
            inputs=[],
            outputs=[],
            resources=Resources(
                minimumCpuCores=1,
                preemptible=None,
                minimumRamGb=4,
                volumes=[
                    Volume(
                        name="user_home",
                        sizeGb=5,
                        source=None,
                        mountPoint="/home/ubuntu",
                        readOnly=False
                    )
                ],
                zones=None
            ),
            docker=[
                DockerExecutor(
                    imageName="jupyterhub/singleuser:latest",
                    cmd=["sh", "/srv/singleuser/singleuser.sh"],
                    workDir=None,
                    stdin=None,
                    stdout=None,
                    stderr=None
                )
            ]
        )
        return clean_task_message(attr.asdict(message))

    @gen.coroutine
    def start(self,
              image=None,
              extra_create_kwargs=None,
              extra_start_kwargs=None,
              extra_host_config=None):
        """Start the single-user server in a docker container via TES. You can
        override the default parameters passed to `create_container` through
        the `extra_create_kwargs` dictionary and passed to `start` through the
        `extra_start_kwargs` dictionary.  You can also override the
        'host_config' parameter passed to `create_container` through the
        `extra_host_config` dictionary.

        Per-instance `extra_create_kwargs`, `extra_start_kwargs`, and
        `extra_host_config` take precedence over their global counterparts.
        """

        # create task message defining notebook server
        message = self._create_message()

        self.log.info(
            "Submitted task: {task} to enpoint: {endpoint}".format(
                task=message,
                endpoint=self._endpoint)
        )

        # post task message to server
        self._jobID = self._post_task(json.dumps(message))

        self.log.info(
            "Started TES job: {jobID}".format(jobID=self._jobID)
        )

        ip, port = yield self.get_ip_and_port()
        # store on user for pre-jupyterhub-0.7:
        self.user.server.ip = ip
        self.user.server.port = port
        # jupyterhub 0.7 prefers returning ip, port:
        return (ip, port)

    @gen.coroutine
    def poll(self):
        """Poll TES worker for status"""
        terminal = ["Complete", "Error", "SystemError", "Canceled"]
        status = self._get_task_status()

        self.log.debug(
            "Status: %s",
            status
        )

        if status not in terminal:
            return None
        else:
            return 1

    @gen.coroutine
    def stop(self, now=False):
        """Stop the TES worker"""
        raise NotImplementedError

    def _post_task(self, json_message):
        """POST task to v1/jobs"""
        response = requests.post(url=self._endpoint, data=json_message)
        response_json = json.loads(response.text)
        jobID = response_json['value']
        return jobID

    def _get_task_status(self):
        """GET v1/jobs/<jobID>"""
        print(self._endpoint, self._jobID)
        endpoint = self._endpoint + "/" + self._jobID
        response = requests.get(url=endpoint)
        response_json = json.loads(response.text)
        # state = response_json['state']
        state = response_json
        return state
