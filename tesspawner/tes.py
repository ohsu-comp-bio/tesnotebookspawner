from attr import attrs, attrib
from attr.validators import instance_of, optional, in_
from .validators import list_of


@attrs
class TaskParameter(object):
    name = attrib(validator=optional(instance_of(str)))
    description = attrib(validator=optional(instance_of(str)))
    url = attrib(validator=instance_of(str))
    path = attrib(validator=instance_of(str))
    type = attrib(validator=in_(["FILE", "DIRECTORY"]))
    contents = attrib(validator=optional(instance_of(str)))


@attrs
class Resources(object):
    cpuCores = attrib(validator=instance_of(int))
    ramGb = attrib(validator=instance_of((float, int)))
    sizeGb = attrib(validator=instance_of((float, int)))
    preemptible = attrib(validator=optional(instance_of(bool)))
    zones = attrib(validator=optional(list_of(str)))


@attrs
class Ports(object):
    host = attrib(validator=instance_of(int))
    container = attrib(validator=instance_of(int))


@attrs
class Executor(object):
    imageName = attrib(validator=instance_of(str))
    cmd = attrib(validator=list_of(str))
    workDir = attrib(validator=optional(instance_of(str)))
    stdin = attrib(validator=optional(instance_of(str)))
    stdout = attrib(validator=optional(instance_of(str)))
    stderr = attrib(validator=optional(instance_of(str)))
    ports = attrib(validator=optional(list_of(Ports)))
    environ = attrib(validator=optional(instance_of(dict)))


@attrs
class Task(object):
    name = attrib(validator=optional(instance_of(str)))
    project = attrib(validator=optional(instance_of(str)))
    description = attrib(validator=optional(instance_of(str)))
    inputs = attrib(validator=list_of(TaskParameter))
    outputs = attrib(validator=list_of(TaskParameter))
    resources = attrib(validator=instance_of(Resources))
    executors = attrib(validator=list_of(Executor))
    volumes = attrib(validator=optional(list_of(str)))
    tags = attrib(validator=optional(instance_of(dict)))


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
