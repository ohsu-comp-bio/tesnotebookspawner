from attr import attrs, attrib
from attr.validators import instance_of, optional
from .validators import list_of


@attrs
class TaskParameter(object):
    name = attrib(validator=optional(instance_of(str)))
    description = attrib(validator=optional(instance_of(str)))
    location = attrib(validator=instance_of(str))
    path = attrib(validator=instance_of(str))
    param_class = attrib(validator=instance_of(str))
    create = attrib(validator=optional(instance_of(bool)))


@attrs
class Volume(object):
    name = attrib(validator=optional(instance_of(str)))
    sizeGb = attrib(validator=instance_of((float, int)))
    source = attrib(validator=optional(instance_of(str)))
    mountPoint = attrib(validator=instance_of(str))
    readOnly = attrib(validator=optional(instance_of(bool)))


@attrs
class Resources(object):
    minimumCpuCores = attrib(validator=instance_of(int))
    preemptible = attrib(validator=optional(instance_of(bool)))
    minimumRamGb = attrib(validator=instance_of((float, int)))
    volumes = attrib(validator=list_of(Volume))
    zones = attrib(validator=optional(list_of(str)))


@attrs
class DockerExecutor(object):
    imageName = attrib(validator=instance_of(str))
    cmd = attrib(validator=list_of(str))
    workDir = attrib(validator=optional(instance_of(str)))
    stdin = attrib(validator=optional(instance_of(str)))
    stdout = attrib(validator=optional(instance_of(str)))
    stderr = attrib(validator=optional(instance_of(str)))
    port = attrib(validator=optional(instance_of(int)))


@attrs
class Task(object):
    name = attrib(validator=optional(instance_of(str)))
    projectID = attrib(validator=optional(instance_of(str)))
    description = attrib(validator=optional(instance_of(str)))
    inputs = attrib(validator=list_of(TaskParameter))
    outputs = attrib(validator=list_of(TaskParameter))
    resources = attrib(validator=instance_of(Resources))
    docker = attrib(validator=list_of(DockerExecutor))


def clean_task_message(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(clean_task_message(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
            (clean_task_message(k), clean_task_message(v))
            for k, v in obj.items() if k is not None and v is not None
        )
    else:
        return obj
