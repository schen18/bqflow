"""
BQFlow SQL Parser Module
Converts BigQuery SQL queries into the JSON config format used by BigQueryExecutor.
"""

import re
from typing import Dict, List, Any, Optional, Tuple
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Token, Parenthesis
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation


class SQLParser:
    """
    Parse SQL queries and convert them to query_config dictionaries.
    
    Example:
        parser = SQLParser()
        
        sql = '''
        SELECT u.user_id, u.name, COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.user_id = o.user_id
        WHERE u.status = 'active' AND o.amount > 100
        GROUP BY u.user_id, u.name
        ORDER BY order_count DESC
        LIMIT 100
        '''
        
        config = parser.parse(sql)
    """
    
    def __init__(self):
        """Initialize the SQL parser."""
        pass
    
    def parse(self, sql: str) -> Dict[str, Any]:
        """
        Parse SQL query into query_config dictionary.
        
        Args:
            sql: SQL query string
        
        Returns:
            Dictionary compatible with BigQueryExecutor.build_query()
        """
        # Clean and format SQL
        sql = sql.strip()
        
        # Parse using sqlparse
        parsed = sqlparse.parse(sql)[0]
        
        config = {}
        
        # Check if query has CTEs
        if self._has_cte(parsed):
            return self._parse_with_cte(sql)
        
        # Extract components
        config['columns'] = self._extract_columns(parsed)
        config['table'] = self._extract_table(parsed)
        
        joins = self._extract_joins(parsed)
        if joins:
            config['joins'] = joins
        
        where = self._extract_where(parsed)
        if where:
            config['where'] = where
        
        group_by = self._extract_group_by(parsed)
        if group_by:
            config['group_by'] = group_by
        
        having = self._extract_having(parsed)
        if having:
            config['having'] = having
        
        order_by = self._extract_order_by(parsed)
        if order_by:
            config['order_by'] = order_by
        
        limit = self._extract_limit(parsed)
        if limit:
            config['limit'] = limit
        
        distinct = self._extract_distinct(parsed)
        if distinct:
            config['distinct'] = True
        
        return config
    
    def _has_cte(self, parsed) -> bool:
        """Check if query contains CTEs (WITH clause)."""
        for token in parsed.tokens:
            if token.ttype is Keyword and token.value.upper() == 'WITH':
                return True
        return False
    
    def _parse_with_cte(self, sql: str) -> Dict[str, Any]:
        """Parse query with CTEs."""
        # Use regex to extract CTEs and main query
        cte_pattern = r'WITH\s+(.*?)\s+SELECT'
        match = re.search(cte_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            # Fallback to raw SQL
            return {'raw_sql': sql}
        
        cte_section = match.group(1)
        main_query_start = match.end() - len('SELECT')
        main_sql = sql[main_query_start:].strip()
        
        # Parse CTEs
        ctes = self._parse_ctes(cte_section)
        
        # Parse main query
        main_parsed = sqlparse.parse(main_sql)[0]
        config = {}
        config['ctes'] = ctes
        config['columns'] = self._extract_columns(main_parsed)
        config['table'] = self._extract_table(main_parsed)
        
        joins = self._extract_joins(main_parsed)
        if joins:
            config['joins'] = joins
        
        where = self._extract_where(main_parsed)
        if where:
            config['where'] = where
        
        group_by = self._extract_group_by(main_parsed)
        if group_by:
            config['group_by'] = group_by
        
        having = self._extract_having(main_parsed)
        if having:
            config['having'] = having
        
        order_by = self._extract_order_by(main_parsed)
        if order_by:
            config['order_by'] = order_by
        
        limit = self._extract_limit(main_parsed)
        if limit:
            config['limit'] = limit
        
        return config
    
    def _parse_ctes(self, cte_section: str) -> List[Dict[str, Any]]:
        """Parse CTE definitions."""
        ctes = []
        
        # Split by comma (but not within parentheses)
        cte_parts = self._split_ctes(cte_section)
        
        for cte_part in cte_parts:
            cte_match = re.match(r'(\w+)\s+AS\s*\((.*)\)', cte_part.strip(), re.IGNORECASE | re.DOTALL)
            if cte_match:
                name = cte_match.group(1)
                cte_sql = cte_match.group(2).strip()
                
                ctes.append({
                    'name': name,
                    'sql': cte_sql
                })
        
        return ctes
    
    def _split_ctes(self, cte_section: str) -> List[str]:
        """Split CTEs by comma, respecting parentheses."""
        parts = []
        current = []
        paren_depth = 0
        
        for char in cte_section:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _extract_distinct(self, parsed) -> bool:
        """Extract DISTINCT keyword."""
        for token in parsed.tokens:
            if token.ttype is Keyword and token.value.upper() == 'DISTINCT':
                return True
        return False
    
    def _extract_columns(self, parsed) -> List[str]:
        """Extract column list from SELECT clause."""
        columns = []
        in_select = False
        
        for token in parsed.tokens:
            if token.ttype is DML and token.value.upper() == 'SELECT':
                in_select = True
                continue
            
            if in_select:
                if token.ttype is Keyword and token.value.upper() in ('FROM', 'INTO'):
                    break
                
                if token.ttype is Keyword and token.value.upper() == 'DISTINCT':
                    continue
                
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        col = self._format_column(identifier)
                        if col:
                            columns.append(col)
                elif isinstance(token, Identifier):
                    col = self._format_column(token)
                    if col:
                        columns.append(col)
                elif token.ttype not in (Whitespace, Punctuation):
                    col = str(token).strip()
                    if col and col != ',':
                        columns.append(col)
        
        return columns if columns else ['*']
    
    def _format_column(self, identifier) -> Optional[str]:
        """Format a column identifier."""
        if identifier.has_alias():
            return f"{identifier.get_real_name()} AS {identifier.get_alias()}"
        return str(identifier).strip()
    
    def _extract_table(self, parsed) -> Optional[str]:
        """Extract table name from FROM clause."""
        from_seen = False
        
        for token in parsed.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    return str(token).strip()
                elif token.ttype not in (Whitespace, Keyword):
                    table_str = str(token).strip()
                    # Stop at JOIN or WHERE
                    if any(kw in table_str.upper() for kw in ['JOIN', 'WHERE', 'GROUP', 'ORDER', 'LIMIT']):
                        break
                    return table_str
            
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
        
        return None
    
    def _extract_joins(self, parsed) -> List[Dict[str, Any]]:
        """Extract JOIN clauses."""
        joins = []
        sql_str = str(parsed)
        
        # Pattern to match JOIN clauses
        join_pattern = r'((?:INNER|LEFT|RIGHT|FULL|CROSS)\s+)?JOIN\s+([^\s]+)\s*(?:AS\s+)?(\w+)?\s+ON\s+([^WHERE|GROUP|ORDER|LIMIT|JOIN]+)'
        
        for match in re.finditer(join_pattern, sql_str, re.IGNORECASE):
            join_type = match.group(1).strip().replace('JOIN', '').strip() if match.group(1) else 'INNER'
            table = match.group(2).strip()
            alias = match.group(3).strip() if match.group(3) else None
            on_clause = match.group(4).strip()
            
            join_config = {
                'type': join_type if join_type else 'INNER',
                'table': table,
                'on': on_clause
            }
            
            if alias:
                join_config['alias'] = alias
            
            joins.append(join_config)
        
        return joins
    
    def _extract_where(self, parsed) -> Optional[Dict[str, Any]]:
        """Extract WHERE conditions."""
        for token in parsed.tokens:
            if isinstance(token, Where):
                return self._parse_conditions(str(token)[6:])  # Remove 'WHERE '
        
        return None
    
    def _parse_conditions(self, condition_str: str) -> Dict[str, Any]:
        """Parse WHERE/HAVING conditions into dictionary."""
        conditions = {}
        
        # Remove leading/trailing whitespace
        condition_str = condition_str.strip()
        
        # Split by AND (simple approach - doesn't handle nested conditions perfectly)
        and_parts = self._split_by_and(condition_str)
        
        for part in and_parts:
            part = part.strip()
            if not part:
                continue
            
            # Try to parse as comparison
            parsed_cond = self._parse_single_condition(part)
            if parsed_cond:
                conditions.update(parsed_cond)
        
        return conditions
    
    def _split_by_and(self, condition_str: str) -> List[str]:
        """Split conditions by AND, respecting parentheses."""
        parts = []
        current = []
        paren_depth = 0
        i = 0
        
        while i < len(condition_str):
            char = condition_str[i]
            
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif paren_depth == 0 and i + 4 <= len(condition_str):
                # Check for AND keyword
                next_chars = condition_str[i:i+4].upper()
                if next_chars == ' AND' or next_chars == 'AND ':
                    parts.append(''.join(current))
                    current = []
                    i += 4
                    continue
            
            current.append(char)
            i += 1
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _parse_single_condition(self, condition: str) -> Optional[Dict[str, Any]]:
        """Parse a single condition."""
        condition = condition.strip()
        
        # Handle IS NULL / IS NOT NULL
        if re.search(r'\s+IS\s+NULL', condition, re.IGNORECASE):
            col = re.sub(r'\s+IS\s+NULL', '', condition, flags=re.IGNORECASE).strip()
            return {col: None}
        
        if re.search(r'\s+IS\s+NOT\s+NULL', condition, re.IGNORECASE):
            col = re.sub(r'\s+IS\s+NOT\s+NULL', '', condition, flags=re.IGNORECASE).strip()
            return {f"{col} IS NOT NULL": None}
        
        # Handle IN operator
        in_match = re.match(r'(.+?)\s+(NOT\s+)?IN\s*\((.+?)\)', condition, re.IGNORECASE)
        if in_match:
            col = in_match.group(1).strip()
            not_in = bool(in_match.group(2))
            values_str = in_match.group(3).strip()
            values = [v.strip().strip("'\"") for v in values_str.split(',')]
            
            key = f"{col} NOT IN" if not_in else f"{col} IN"
            return {key: values}
        
        # Handle BETWEEN operator
        between_match = re.match(r'(.+?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)', condition, re.IGNORECASE)
        if between_match:
            col = between_match.group(1).strip()
            start = between_match.group(2).strip().strip("'\"")
            end = between_match.group(3).strip().strip("'\"")
            return {f"{col} BETWEEN": [start, end]}
        
        # Handle LIKE operator
        like_match = re.match(r'(.+?)\s+LIKE\s+(.+)', condition, re.IGNORECASE)
        if like_match:
            col = like_match.group(1).strip()
            pattern = like_match.group(2).strip().strip("'\"")
            return {f"{col} LIKE": pattern}
        
        # Handle comparison operators (>=, <=, !=, <>, >, <, =)
        for op in ['>=', '<=', '!=', '<>', '>', '<', '=']:
            if op in condition:
                parts = condition.split(op, 1)
                if len(parts) == 2:
                    col = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    
                    # Try to convert to number
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        pass
                    
                    if op == '=':
                        return {col: value}
                    else:
                        return {f"{col} {op}": value}
        
        return None
    
    def _extract_group_by(self, parsed) -> Optional[List[str]]:
        """Extract GROUP BY columns."""
        sql_str = str(parsed)
        match = re.search(r'GROUP\s+BY\s+(.+?)(?:HAVING|ORDER|LIMIT|$)', sql_str, re.IGNORECASE | re.DOTALL)
        
        if match:
            group_cols = match.group(1).strip()
            return [col.strip() for col in group_cols.split(',')]
        
        return None
    
    def _extract_having(self, parsed) -> Optional[Dict[str, Any]]:
        """Extract HAVING conditions."""
        sql_str = str(parsed)
        match = re.search(r'HAVING\s+(.+?)(?:ORDER|LIMIT|$)', sql_str, re.IGNORECASE | re.DOTALL)
        
        if match:
            having_str = match.group(1).strip()
            return self._parse_conditions(having_str)
        
        return None
    
    def _extract_order_by(self, parsed) -> Optional[List[str]]:
        """Extract ORDER BY columns."""
        sql_str = str(parsed)
        match = re.search(r'ORDER\s+BY\s+(.+?)(?:LIMIT|$)', sql_str, re.IGNORECASE | re.DOTALL)
        
        if match:
            order_cols = match.group(1).strip()
            return [col.strip() for col in order_cols.split(',')]
        
        return None
    
    def _extract_limit(self, parsed) -> Optional[int]:
        """Extract LIMIT value."""
        sql_str = str(parsed)
        match = re.search(r'LIMIT\s+(\d+)', sql_str, re.IGNORECASE)
        
        if match:
            return int(match.group(1))
        
        return None


def sql_to_config(sql: str) -> Dict[str, Any]:
    """
    Convenience function to convert SQL to query config.
    
    Args:
        sql: SQL query string
    
    Returns:
        Query configuration dictionary
    """
    parser = SQLParser()
    return parser.parse(sql)


if __name__ == "__main__":
    import json
    
    parser = SQLParser()
    
    # Example 1: Simple query
    sql1 = """
    SELECT user_id, name, email
    FROM users
    WHERE status = 'active' AND created_date > '2024-01-01'
    ORDER BY created_date DESC
    LIMIT 100
    """
    
    config1 = parser.parse(sql1)
    print("Example 1: Simple Query")
    print(json.dumps(config1, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Example 2: Query with joins
    sql2 = """
    SELECT 
        u.user_id,
        u.name,
        COUNT(o.order_id) as order_count,
        SUM(o.amount) as total_amount
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    WHERE u.status = 'active' 
        AND o.amount > 100
    GROUP BY u.user_id, u.name
    HAVING COUNT(o.order_id) > 5
    ORDER BY total_amount DESC
    """
    
    config2 = parser.parse(sql2)
    print("Example 2: Query with Joins")
    print(json.dumps(config2, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Example 3: Query with multiple joins
    sql3 = """
    SELECT DISTINCT
        u.user_id,
        u.name,
        p.product_name,
        SUM(oi.quantity) as total_quantity
    FROM users u
    INNER JOIN orders o ON u.user_id = o.user_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products p ON oi.product_id = p.product_id
    WHERE u.status IN ('active', 'premium')
        AND o.order_date >= '2024-01-01'
        AND oi.quantity > 0
    GROUP BY u.user_id, u.name, p.product_name
    ORDER BY total_quantity DESC
    LIMIT 50
    """
    
    config3 = parser.parse(sql3)
    print("Example 3: Multiple Joins with DISTINCT")
    print(json.dumps(config3, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Example 4: Query with CTE
    sql4 = """
    WITH active_users AS (
        SELECT user_id, name, email
        FROM users
        WHERE status = 'active'
    ),
    user_orders AS (
        SELECT 
            user_id,
            COUNT(order_id) as order_count,
            SUM(amount) as total_amount
        FROM orders
        WHERE order_date > '2024-01-01'
        GROUP BY user_id
    )
    SELECT 
        au.user_id,
        au.name,
        uo.order_count,
        uo.total_amount
    FROM active_users au
    INNER JOIN user_orders uo ON au.user_id = uo.user_id
    WHERE uo.total_amount > 1000
    ORDER BY uo.total_amount DESC
    """
    
    config4 = parser.parse(sql4)
    print("Example 4: Query with CTEs")
    print(json.dumps(config4, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Example 5: Complex operators
    sql5 = """
    SELECT *
    FROM orders
    WHERE status IN ('pending', 'processing')
        AND amount BETWEEN 100 AND 5000
        AND customer_name LIKE '%Smith%'
        AND discount != 0
        AND created_at >= '2024-01-01'
        AND deleted_at IS NULL
    ORDER BY amount DESC
    """
    
    config5 = parser.parse(sql5)
    print("Example 5: Complex Operators")
    print(json.dumps(config5, indent=2))