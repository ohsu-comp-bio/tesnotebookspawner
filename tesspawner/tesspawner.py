"""
A Spawner for JupyterHub that runs each user's server in a separate docker
container spun up by TES
"""

import attr
import json
import requests
import time

from tornado import gen
from jupyterhub.spawner import Spawner
from traitlets import (
    Dict,
    Unicode,
    Bool,
    Integer,
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
    # override default since batch systems typically need longer
    start_timeout = Integer(300, config=True)

    endpoint = Unicode("http://127.0.0.1:6000/v1/jobs",
                       help="TES server endpoint").tag(config=True)

    container_image = Unicode("jupyterhub/singleuser:latest").tag(config=True)
    notebook_command = Unicode("bash /srv/singleuser/singleuser.sh").tag(config=True)

    task_id = Unicode("")
    status = Unicode("")

    def _create_message(self):
        message = Task(
            name=self.container_image,
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
                    imageName=self.container_image,
                    cmd=["bash", "-c", "{}".format(self.build_command())],
                    workDir=None,
                    stdin=None,
                    stdout="stdout",
                    stderr="stderr"
                )
            ]
        )

        return clean_task_message(attr.asdict(message))

    def get_env(self):
        env = super(TesSpawner, self).get_env()
        env.update(dict(
            JPY_USER=self.user.name,
            JPY_COOKIE_NAME=self.user.server.cookie_name,
            JPY_BASE_URL=self.user.server.base_url,
            JPY_HUB_PREFIX=self.hub.server.base_url,
            JPY_HUB_API_URL=self.hub.api_url
        ))

        if self.notebook_dir:
            env['NOTEBOOK_DIR'] = self.notebook_dir

        return env

    def build_command(self):
        exports = ["export"]
        env = self.get_env()
        whitelist = ["JPY_API_TOKEN", "JPY_BASE_URL", "JPY_COOKIE_NAME",
                     "JPY_HUB_API_URL", "JPY_HUB_PREFIX", "JPY_USER",
                     "NOTEBOOK_DIR"]
        for k, v in env.items():
            if k in whitelist:
                exports.append("{0}={1}".format(k, v))

        cmd = " ".join(exports) + " && " + self.notebook_command
        return cmd

    def load_state(self, state):
        """load task_id from state"""
        super(TesSpawner, self).load_state(state)
        self.task_id = state.get('task_id', '')
        self.status = state.get('status', '')

    def get_state(self):
        """add task_id to state"""
        state = super(TesSpawner, self).get_state()
        if self.task_id:
            state['task_id'] = self.task_id
        if self.status:
            state['status'] = self.status
        return state

    def clear_state(self):
        """clear job_id state"""
        super(TesSpawner, self).clear_state()
        self.task_id = ""
        self.status = ''

    @gen.coroutine
    def start(self):
        """Start the single-user server in a docker container via TES."""

        # create task message defining notebook server
        message = self._create_message()

        self.log.info(
            "Submititng task: {task} to {endpoint}".format(
                task=message,
                endpoint=self.endpoint)
        )

        # post task message to server
        self._post_task(json.dumps(message))

        self.log.info(
            "Started TES job: {0}".format(self.task_id)
        )
        time.sleep(5)
        ip, port = yield self._get_ip_and_port(0)
        return (ip, port)

    @gen.coroutine
    def poll(self):
        terminal = ["Complete", "Error", "SystemError", "Canceled"]
        self._get_task_status()

        self.log.debug(
            "Job {0} status: {1}".format(self.task_id, self.status)
        )

        if self.status not in terminal:
            return None
        else:
            return 1

    @gen.coroutine
    def stop(self, now=False):
        """Stop the TES worker"""
        raise NotImplementedError

    @gen.coroutine
    def _post_task(self, json_message):
        """POST task to v1/jobs"""
        response = requests.post(url=self.endpoint, data=json_message)
        if response.status_code // 100 != 2:
            raise RuntimeError("[ERROR] {0}".format(response.text))
        response_json = json.loads(response.text)
        self.task_id = response_json['value']
        return response

    @gen.coroutine
    def _get_task(self):
        """GET v1/jobs/<jobID>"""
        self.log.debug(
            "GET {0}/{1}".format(self.endpoint, self.task_id)
        )
        endpoint = self.endpoint + "/" + self.task_id
        response = requests.get(url=endpoint)
        if response.status_code // 100 != 2:
            raise RuntimeError("[ERROR] {0}".format(response.text))
        return response

    @gen.coroutine
    def _get_task_status(self):
        if self.task_id is None or len(self.task_id) == 0:
            # job not running
            self.status = ""
            return self.status

        response = yield self._get_task()
        response_json = json.loads(response.text)
        status = response_json["state"]
        self.status = status
        return self.status

    @gen.coroutine
    def _get_ip_and_port(self, retries):
        if retries >= 10:
            raise RuntimeError("Failed to get the ip and port of the docker container")

        response = yield self._get_task()
        response_json = json.loads(response.text)

        if "metadata" not in response_json:
            time.sleep(1)
            return gen.Return(self._get_ip_and_port(retries + 1))

        # suffix added to task_id to reflect step since TES supports an array
        # of DockerExecutors
        self.log.info(
            "Metadata for job: {0}".format(response_json)
        )

        task_meta = json.loads(response_json["metadata"][self.task_id + "0"])
        ip = task_meta['NetworkSettings']['IPAddress']

        # TODO handle errors better
        if len(task_meta['HostConfig']['PortBindings'].keys()) != 1:
            raise RuntimeError("Container has more than one port binding")

        for k, v in task_meta['HostConfig']['PortBindings'].items():
            port = v[0]['HostPort']
        return ip, port
