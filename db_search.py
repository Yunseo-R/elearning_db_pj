import mariadb
from db_connector import DBConnector
from typing import List, Dict, Any, Optional, Tuple

class DataSearcher(DBConnector):
    """DB에서 정보를 검색하는 클래스"""
    def __init__(self):
        """DataSearcher 클래스 초기화"""
        super().__init__()

    def get_available_years_and_semesters(self) -> List[Tuple[int, int]]:
        """
        데이터베이스의 연도/학기 목록 조회

        Returns:
            List[Tuple[int, int]]: (연도, 학기) 튜플의 리스트
            예: [(2024, 1), (2024, 2), (2023, 1), (2023, 2)]
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT year, semester 
                FROM TB_ELEARNING_DATA 
                ORDER BY year DESC, semester DESC
            """)
            result = cursor.fetchall()
            conn.close()
            return result

        except mariadb.Error as e:
            print(f"연도/학기 목록 조회 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def search_by_name_or_birthday(self, year: int, semester: int,
                                   name: Optional[str] = None,
                                   birthday: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        이름 또는 생년월일로 검색

        Args:
            year (int): 연도
            semester (int): 학기
            name (str, optional): 이름
            birthday (str, optional): 생년월일(예: '990203-1')

        Returns:
            List[Dict[str, Any]]: 검색 결과 목록
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            query = """
                SELECT 
                    agency_number, agency_name, t_name, birthday,
                    nice_number, affiliation, area, ele_school,
                    mid_school, start_date, end_date
                FROM TB_ELEARNING_DATA
                WHERE year = ? AND semester = ?
            """
            params = [year, semester]

            if name:
                query += " AND t_name LIKE ?"
                params.append(f"%{name}%")

            if birthday:
                query += " AND birthday = ?"
                params.append(birthday)

            query += " ORDER BY t_name ASC"
            cursor.execute(query, params)

            columns = [desc[0] for desc in cursor.description]
            result = []
            for row in cursor.fetchall():
                result.append(dict(zip(columns, row)))

            conn.close()
            return result

        except mariadb.Error as e:
            print(f"데이터 검색 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []
