class Data(object):
    def __init__(self, id=None, value=None, parents=None, func_name=None, \
        size=None, time=None, cpu_time=0., dltime=0., workflow=None,
        n_input=0, n_output=0, node=-1, inputs=None, outputs=None):
        self.id = id
        self.value = value
        self.parents = parents
        self.func_name = func_name
        self.size = size
        self.time = time
        self.cpu_time = cpu_time
        self.dltime = dltime
        self.workflow = workflow
        self.n_input=n_input
        self.n_output=n_output
        self.node=node
        self.inputs=inputs
        self.outputs=outputs

    def __set__(self, instance, value):
        self.instance = value

#####     Id functions    ####""""""""""""""""""""""""""""""""""""""""""""""""""
def set_id(data):
    if data.id:
        return data.id
    else:
        # get func and input
        if not data.parents:
            new_id = "orphan"
            new_id += str(data.workflow)
            data.id = new_id
        else:
            new_id = ""
            new_id += str(data.workflow)
            new_id += data.func_name
            new_id += "("
            for parent in data.parents:
                if isinstance(parent, Data):
                    set_id(parent)
                    new_id += parent.id
                    new_id += ","
            #                 else:
            #                     try:
            #                         new_id+=repr(parent)
            #                     except:
            #                         pass
            new_id += ")"
            data.id = new_id
        return data.id


def get_task_id(func, *args, **kwargs):
    new_data = Data(id=None, value=None, parents=args, func_name=str(func.__name__))
    return set_id(new_data)