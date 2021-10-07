class Response:
    """
    Response to a request from the client to call a method
    """
    def __init__(self, handlers, resources):
        self._handlers = handlers
        self._resources = resources

    def handler(self, name: str):
        """
        Retrieve a handler

        :param name: the name of the handler
        """
        return self._handlers[name]

    def resource(self, name: str):
        """
        Retrieve a resource

        :param name: the name of the resource
        """
        return self._resources[name]

    def load(self, name: str):
        """
        Loads a resource using its handler

        :param name: the name of the resource and handler
        """
        handler = self.handler(name)
        resource = self.resource(name)
        return handler.load(resource)

    def save(self, name: str, data):
        """
        Saves a resource using its handler

        :param name: the name of the resource and handler
        :param data: the data to save
        """
        handler = self.handler(name)
        resource = self.resource(name)
        return handler.save(resource, data)