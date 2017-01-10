from attr import attrs, attrib


@attrs
class _ListOfValidator(object):
    type = attrib()

    def __call__(self, inst, attr, value):
        """
        We use a callable class to be able to change the ``__repr__``.
        """
        if not all([isinstance(n, self.type) for n in value]):
            raise TypeError(
                "'{name}' must be a list of {type!r} (got {value!r} that is a "
                "list of {actual!r})."
                .format(name=attr.name,
                        type=self.type,
                        actual=value[0].__class__,
                        value=value),
                attr, self.type, value,
            )

    def __repr__(self):
        return (
            "<instance_of validator for type {type!r}>"
            .format(type=self.type)
        )


def list_of(type):
    return _ListOfValidator(type)
