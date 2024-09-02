from dataclasses import dataclass
import dataclasses
from typing import List, Optional


class Operation:
    UPDATE = "UPDATE"
    INSERT = "INSERT"
    DELETE = "DELETE"


@dataclass(frozen=True, order=True)
class S4Vector:
    ssn: int
    sum: int
    sid: int
    seq: int

    @classmethod
    def from_vector(cls, sid, vector): 
        return cls(
            ssn=1, #TODO: assume all vector using same session now
            sid=sid,
            sum= sum(vector),
            seq= vector[sid]
        )

    def to_dict(self):
        return dataclasses.asdict(self)

@dataclass
class OperationPkg:
    operation: str
    obj: Optional[str]
    index: Optional[S4Vector]
    from_sid: int
    vector: List[int]

    def to_dict(self):
        return dataclasses.asdict(self)

@dataclass
class Node:
    obj: str
    sk: S4Vector
    sp: S4Vector
    # next: "Node" #next is for hash collision, no need in python
    link: Optional["Node"] = None
