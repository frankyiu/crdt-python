from typing import Dict, Optional 
from datatypes import Node, S4Vector

class RGA:

    def __init__(self):
        self.head: Node = None
        self.hash_table: Dict[S4Vector, Node] = {}

    def __str__(self):
        text = ""
        cur = self.head
        while cur:
            if cur.obj:
                text += cur.obj
            cur = cur.link
        return text

    def find_list(self, index):
        if index == 0:
            return None
        cur = self.head
        k = 0
        while cur:
            if cur.obj != None:
                k += 1
                if index == k:
                    return cur
            cur = cur.link
        return None

    def find_link(self, n):
        return n if n.obj else None

    def insert(self, index, obj_vector: S4Vector, obj):
        if index != 0:
            ref = self.find_list(index)
            if not ref:
                raise Exception("No Ref Exception")
        ins = Node(sk=obj_vector, sp=obj_vector, obj=obj)
        self.hash_table[obj_vector] = ins
        if index == 0:
            ins.link = self.head
            self.head = ins
            return True
        ins.link = ref.link
        ref.link = ins
        return True

    def delete(self, index, obj_vector: S4Vector):
        n = self.find_list(index)
        if not n:
            return False
        n.obj = None
        n.sp = obj_vector
        return True

    def update(self, index, obj_vector: S4Vector, obj):
        n = self.find_list(index)
        if not n:
            return False
        n.obj = obj
        n.sp = obj_vector
        return True

    def read(self, index):
        n = self.find_list(index)
        return n.obj if n else None

    def remote_insert(self, index: Optional[S4Vector], obj_vector: S4Vector, obj):
        if index:
            ref = self.hash_table[index]
            if not ref:
                raise Exception("No Ref Exception")
        ins = Node(sk=obj_vector, sp=obj_vector, obj=obj)
        self.hash_table[obj_vector] = ins

        if not index:
            if not self.head or self.head.sk < ins.sk:
                if self.head:
                    ins.link = self.head
                self.head = ins
                return True
            ref = self.head
        while ref.link and ins.sk < ref.link.sk:
            ref = ref.link
        ins.link = ref.link
        ref.link = ins
        return True

    def remote_delete(self, index: S4Vector, obj_vector: S4Vector):
        n = self.hash_table[index]
        if not n:
            raise Exception("No Target Object Exception")
        if self.find_link(n):
            n.obj = None
            n.sp = obj_vector
            # Cemetery enrol
        return True

    def remote_update(self, index: S4Vector, obj_vector: S4Vector, obj):
        n = self.hash_table[index]
        if not n:
            raise Exception("No Target Object Exception")
        if not self.find_link(n) or obj_vector < n.sp:
            return False
        n.obj = obj
        n.sp = obj_vector
        return True

