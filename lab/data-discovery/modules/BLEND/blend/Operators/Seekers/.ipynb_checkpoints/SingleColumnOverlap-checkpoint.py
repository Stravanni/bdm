from blend.Operators.Seekers.SeekerBase import Seeker

# Typing imports
from blend.DBHandler import DBHandler
from typing import Iterable


class SingleColumnOverlap(Seeker):
    def __init__(self, input_query_values: Iterable[str], k: int = 10, verbosity: int = 1) -> None:
        assert 1 <= verbosity <= 3
        super().__init__(k, verbosity)
        
        self.input = set(input_query_values)
        self.base_sql = """
        SELECT $VERBOSITY$ FROM AllTables
        WHERE CellValue IN ($TOKENS$) $ADDITIONALS$
        GROUP BY TableId, ColumnId
        ORDER BY COUNT(DISTINCT CellValue) DESC
        LIMIT $TOPK$
        """

    def create_sql_query(self, db: DBHandler, additionals: str = "") -> str:
        v = ','.join(['TableId', 'ColumnId', 'COUNT(DISTINCT CellValue)'][:self.verbosity])
        
        sql = self.base_sql.replace('$VERBOSITY$', v)
        sql = sql.replace('$TOPK$', str(self.k))
        sql = sql.replace('$ADDITIONALS$', additionals)
        sql = sql.replace('$TOKENS$', db.create_sql_list_str(db.clean_value_collection(self.input)))

        return sql

    def cost(self) -> int:
        return 4
    
    def ml_cost(self, db: DBHandler) -> float:
        return self._predict_runtime([list(self.input)], db)
