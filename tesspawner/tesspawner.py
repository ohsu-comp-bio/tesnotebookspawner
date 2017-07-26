"""
A Spawner for JupyterHub that runs each user's server in a separate docker
container spun up by TES
"""

import polling

from tornado import gen
from jupyterhub.spawner import Spawner
from traitlets import (
    Unicode,
    Integer,
    default,
    observe
)
from tes import (
    Task,
    TaskParameter,
    Resources,
    Ports,
    Executor,
    HTTPClient
)


class TesSpawner(Spawner):
    # override default since TES may need longer
    start_timeout = Integer(300, config=True)
    endpoint = Unicode(help="TES server endpoint").tag(config=True)
    notebook_command = Unicode(
        "bash /usr/local/bin/start-singleuser.sh"
    ).tag(config=False)
    task_id = Unicode().tag(config=False)
    status = Unicode().tag(config=False)
    _client = None

    @observe("endpoint")
    def init_client(self, change):
        self._client = HTTPClient(change["new"])

    @default("options_form")
    def _options_form_default(self):
        return """
        <label for="image">Docker Image</label>
        <select name="image">
          <option value="jupyter/datascience-notebook:latest" selected>jupyter/datascience-notebook</option>
          <option value="jupyter/tensorflow-notebook:latest">jupyter/tensorflow-notebook</option>
        </select>
        <label for="cpu">CPU</label>
        <input name="cpu" placeholder="1"></input>
        <label for="mem">RAM (GB)</label>
        <input name="mem" placeholder="8"></input>
        <label for="disk">Disk Size (GB)</label>
        <input name="disk" placeholder="10"></input>
        """

    @staticmethod
    def _process_option(v, default_val, typef):
        if (v == "") or (v is None):
            return default_val
        else:
            return typef(v)

    def options_from_form(self, formdata):
        """Handle user specifed options"""
        self.log.info("Form data: {}".format(formdata))
        options = {}
        options["cpu"] = self._process_option(
            formdata.get("cpu", [""])[0], 1, int
        )
        options["mem"] = self._process_option(
            formdata.get("mem", [""])[0], 8, float
        )
        options["disk"] = self._process_option(
            formdata.get("disk", [""])[0], 10, float
        )
        options["image"] = self._process_option(
            formdata.get("image", [""])[0],
            "jupyter/datascience-notebook:latest",
            str
        )
        self.log.info("Parsed options: {}".format(options))
        return options

    def _create_message(self):
        """Generate a TES Task message"""
        image = self._process_option(
            self.user_options.get("image"),
            "jupyter/datascience-notebook:latest",
            str
        )

        message = Task(
            name=image,
            inputs=[],
            outputs=[],
            resources=Resources(
                cpu_cores=self._process_option(
                    self.user_options.get("cpu"), 1, int
                ),
                ram_gb=self._process_option(
                    self.user_options.get("mem"), 8, float
                    ),
                size_gb=self._process_option(
                    self.user_options.get("disk"), 10, float
                ),
            ),
            executors=[
                Executor(
                    image_name=image,
                    cmd=["bash", "-c", self.notebook_command],
                    workdir="/home/jovyan/work",
                    stdout="/home/jovyan/work/stdout",
                    stderr="/home/jovyan/work/stderr",
                    ports=[
                        Ports(
                            host=0,
                            container=8888
                        )
                    ],
                    environ=self._get_env()
                )
            ]
        )

        return message

    def _get_env(self):
        """get the needed jupyterhub enviromental varaibles"""
        env = super(TesSpawner, self).get_env()
        env.update(dict(
            JPY_USER=self.user.name,
            JPY_COOKIE_NAME=self.user.server.cookie_name,
            JPY_BASE_URL=self.user.server.base_url,
            JPY_HUB_PREFIX=self.hub.server.base_url,
            JPY_HUB_API_URL=self.hub.api_url
        ))

        if self.notebook_dir:
            env["NOTEBOOK_DIR"] = self.notebook_dir

        whitelist = ["JPY_API_TOKEN", "JPY_BASE_URL", "JPY_COOKIE_NAME",
                     "JPY_HUB_API_URL", "JPY_HUB_PREFIX", "JPY_USER",
                     "NOTEBOOK_DIR"]
        filtered_env = {}
        for k, v in env.items():
            if k in whitelist:
                filtered_env[k] = v

        return filtered_env

    def load_state(self, state):
        """load task_id from state"""
        super(TesSpawner, self).load_state(state)
        self.task_id = state.get("task_id", "")
        self.status = state.get("status", "")

    def get_state(self):
        """add task_id to state"""
        state = super(TesSpawner, self).get_state()
        if self.task_id:
            state["task_id"] = self.task_id
        if self.status:
            state["status"] = self.status
        return state

    def clear_state(self):
        """clear job_id state"""
        super(TesSpawner, self).clear_state()
        self.task_id = ""
        self.status = ""

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
        self.task_id = self._client.create_task(message)

        self.log.info(
            "Started TES job: {0}".format(self.task_id)
        )

        ip, port = self._get_ip_and_port(0)
        return (ip, port)

    @gen.coroutine
    def poll(self):
        terminal = ["COMPLETE", "ERROR", "SYSTEM_ERROR", "CANCELED"]
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
        if self.task_id != "":
            return self._client.cancel_task(self.task_id)
        else:
            return

    def _get_task_status(self):
        if self.task_id == "":
            # job not running
            self.status = ""
            return self.status

        response = self._client.get_task(self.task_id, "MINIMAL")
        self.status = response.state
        return

    def _get_ip_and_port(self, timeout=60):
        def check_success(r):
            if r.logs is not None:
                if r.logs[0].logs is not None:
                    s1 = r.logs[0].logs[0].host_ip is not None
                    s2 = r.logs[0].logs[0].ports is not None
                    if s1 and s2:
                        if r.logs[0].logs[0].ports[0].host is not None:
                            return True
            return False

        r = polling.poll(
            lambda: self._client.get_task(self.task_id, "FULL"),
            check_success=check_success,
            step=0.1,
            timeout=timeout
        )

        ip = r.logs[0].logs[0].host_ip
        port = r.logs[0].logs[0].ports[0].host
        return ip, port
