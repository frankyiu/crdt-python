import json
from typing import Dict, List, Optional 
from datatypes import Node, Operation, OperationPkg, S4Vector
from dacite import from_dict
from model.rga import RGA



class RGAService:

    def __init__(self, site_id, num_sites, broadcaster):
        self.site_id = site_id
        self.vector = [0] * num_sites
        self.rga = RGA()
        self.operation_queue: List[OperationPkg] = []
        self.broadcaster = broadcaster

    def check_causally(self):
        i = 0
        while i < len(self.operation_queue):
            q_operation = self.operation_queue[i]
            q_vector = q_operation.vector
            from_sid = q_operation.from_sid
            causally = True
            for j, (a, b) in enumerate(zip(q_vector, self.vector)):
                if j != from_sid and a > b:
                    causally = False
                    break
            if (q_vector[from_sid] == self.vector[from_sid] + 1) and causally:
                return self.operation_queue.pop(i)
            i += 1
        return None

    def income_operation(self, operation_pkg: OperationPkg):
        self.operation_queue.append(operation_pkg)
        while (operation_pkg := self.check_causally()) is not None:
            self.vector = [max(a, b) for a, b in zip(self.vector, operation_pkg.vector)]
            # remote operation
            object_vector = S4Vector.from_vector(operation_pkg.from_sid, operation_pkg.vector)
            if operation_pkg.operation == Operation.INSERT:
                self.rga.remote_insert(index=operation_pkg.index, obj_vector=object_vector, obj=operation_pkg.obj)
            elif operation_pkg.operation == Operation.UPDATE:
                self.rga.remote_update(index=operation_pkg.index, obj_vector=object_vector, obj=operation_pkg.obj)
            elif operation_pkg.operation == Operation.DELETE: 
                self.rga.remote_delete(index=operation_pkg.index, obj_vector=object_vector)

    def to_view(self):
        return str(self.rga)

    def insert(self, index, obj):
        operation_pkg = self._prepare_operation_pkg(operation=Operation.INSERT, index=index, obj=obj)
        self.rga.insert(index=index, obj_vector=self._prepare_object_vector(), obj=obj)
        self.broadcast(operation_pkg=operation_pkg)

    def update(self, index, obj):
        operation_pkg= self._prepare_operation_pkg(operation=Operation.UPDATE, index=index, obj=obj)
        self.rga.update(index=index, obj_vector=self._prepare_object_vector(), obj=obj)
        self.broadcast(operation_pkg=operation_pkg)

    def delete(self, index):
        operation_pkg = self._prepare_operation_pkg(operation=Operation.DELETE, index=index, obj=None)
        self.rga.delete(index=index, obj_vector=self._prepare_object_vector() )
        self.broadcast(operation_pkg=operation_pkg)
    
    def read(self, index):
        return self.rga.read(index)

    def _prepare_object_vector(self):
        return S4Vector.from_vector(self.site_id, self.vector)
    
    def _prepare_operation_pkg(self, operation, index, obj=None):
        self.vector[self.site_id] += 1
        node = self.rga.find_list(index)
        operation = OperationPkg(
            operation=operation,
            obj=obj,
            index=node.sk if node else None,
            from_sid=self.site_id,
            vector=self.vector
        )
        return operation

    def broadcast(self, operation_pkg):
        message = json.dumps(operation_pkg.to_dict())
        self.broadcaster.broadcast(message, self.site_id)
    
    def receive(self, message):
        data = json.loads(message)
        operation_pkg = from_dict(data_class=OperationPkg, data=data)
        self.income_operation(operation_pkg=operation_pkg)
        
