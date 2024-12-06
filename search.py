import mariadb
from db_connector import DBConnector
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd


class DataSearcher(DBConnector):
    """
    DB 정보 검색 클래스
    DBConnector를 상속받아 DB연결을 관리
    """

    def __init__(self):
        """DataSearcher 클래스 초기화"""
        super().__init__()

    def get_available_years_and_semesters(self) -> List[Tuple[int, int]]:
        """
        데이터베이스의 연도/학기 목록 조회

        Returns:
            List[Tuple[int, int]]: (연도, 학기) 튜플의 리스트
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
            return result

        except mariadb.Error as e:
            print(f"연도/학기 목록 조회 중 오류 발생: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()

    def search_by_name_or_birthday(self, year: int, semester: int,
                                   name: Optional[str] = None,
                                   birthday: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        이름 또는 생년월일로 데이터를 검색하는 메서드

        Args:
            year (int): 검색 대상 연도
            semester (int): 검색 대상 학기
            name (str, optional): 검색할 이름 (정확히 일치해야 함)
            birthday (str, optional): 검색할 생년월일 (형식: 'YYYYMMDD-숫자')

        Returns:
            List[Dict[str, Any]]: 검색된 데이터 목록. 각 데이터는 딕셔너리 형태로 반환됨.
                                  예: [{'agency_name': '...', 't_name': '...', ...}, {...}]

        Raises:
            mariadb.Error: 데이터베이스 쿼리 중 오류가 발생할 경우 예외 발생
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()

            # 쿼리
            query = """
                SELECT 
                    year, semester, agency_name, affiliation, agency_number,
                    school_type, t_name, nice_number, birthday, area,
                    start_date, end_date, reg_date, remarks
                FROM TB_ELEARNING_DATA
                WHERE year = ? AND semester = ?
            """
            params = [year, semester]

            # 이름 검색 조건 추가 (정확히 일치하는 이름만 검색)
            if name:
                # 이름 검색어 일부 포함도 검색
                # query += " AND t_name LIKE ?"
                # params.append(f"%{name}%")
                query += " AND t_name = ?"
                params.append(name)

            # 생년월일 검색 조건
            if birthday:
                query += " AND birthday = ?"
                params.append(birthday)

            # 정렬 조건
            query += " ORDER BY t_name ASC"
            # 쿼리 실행
            cursor.execute(query, params)

            # 컬럼 이름과 데이터를 매핑해 딕셔너리로 변환
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]

            conn.close()
            return result

        except mariadb.Error as e:
            print(f"데이터 검색 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []




class ResultDownloader:
    """검색 결과 다운로드 클래스"""

    def __init__(self):
        """ResultDownloader 초기화"""
        pass

    def download_to_csv(self, data: List[Dict[str, Any]], file_path: str) -> None:
        """
        검색 결과 데이터를 CSV 파일로 저장

        Args:
            data (List[Dict[str, Any]]): 저장할 검색 결과
            file_path (str): 저장할 CSV 파일 경로
        """
        if not data:
            raise ValueError("저장할 데이터가 비어 있습니다.")

        # pandas DataFrame으로 변환
        df = pd.DataFrame(data)
        try:
            # CSV 파일로 저장
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"검색 결과가 CSV 파일로 저장되었습니다: {file_path}")
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")
            raise
