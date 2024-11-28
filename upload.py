import mariadb
import pandas as pd
import re
from datetime import datetime
from db_connector import DBConnector

class DataImporter(DBConnector):
    """
    엑셀 파일에서 데이터를 읽어 DB에 삽입하는 클래스
    DB에 연결하기 위한 기본 기능을 DBConnector로부터 상속받습니다.
    엑셀 파일의 각 시트('초'/'중')에서 데이터를 읽어와 DB에 저장합니다.
    """
    def __init__(self, excel_file_path):
        """
        DataImporter 클래스의 초기화 메서드
        Args:
            excel_file_path (str): 읽어올 엑셀 파일의 경로
                                 예: "C:/Users/username/Desktop/data.xlsx"
        부모 클래스(DBConnector)의 초기화 메서드도 함께 호출합니다.
        """
        super().__init__()
        self.excel_file_path = excel_file_path

    def extract_year_semester(self, sheet_header):
        """
        시트 헤더에서 연도와 학기 정보를 추출하는 메서드
        Args:
            sheet_header (str): 엑셀 시트의 첫 번째 셀 내용
                              예: "2024학년도 1학기"
        Returns:
            tuple: (연도, 학기) 형태의 튜플
                  예: (2024, 1)
        Raises:
            ValueError: 연도나 학기 정보를 추출할 수 없을 때 발생
        """
        year_match = re.search(r'(\d{4})학년도', sheet_header)
        semester_match = re.search(r'(\d)학기', sheet_header)

        if not year_match:
            raise ValueError("시트 제목에서 연도를 추출할 수 없습니다.")
        if not semester_match:
            raise ValueError("시트 제목에서 학기를 추출할 수 없습니다.")

        return int(year_match.group(1)), int(semester_match.group(1))

    def insert_data(self, conn, data, year, semester, ele_school, mid_school):
        """
        데이터를 DB에 삽입하는 메서드
        
        Args:
            conn (mariadb.connection): 데이터베이스 연결 객체
            data (pandas.DataFrame): 삽입할 데이터가 담긴 DataFrame
            year (int): 해당 데이터의 연도
            semester (int): 해당 데이터의 학기
            ele_school (str): 초등학교 여부 ('초' 또는 None)
            mid_school (str): 중학교 여부 ('중' 또는 None)
        
        DataFrame의 각 행을 순회하며 DB에 삽입합니다.
        소속과 성명이 모두 없는 행은 건너뜁니다.
        삽입 중 오류 발생 시 롤백하여 데이터 일관성을 유지합니다.
        """
        cursor = conn.cursor()
        query = ("INSERT INTO TB_ELEARNING_DATA (year, semester, agency_name, affiliation, "
                 "agency_number, ele_school, mid_school, t_name, nice_number, birthday, "
                 "area, start_date, end_date, reg_date, remarks) "
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

        try:
            for _, row in data.iterrows():
                # 소속과 성명이 모두 비어있는 행은 건너뛰기
                if pd.isna(row['소속']) and pd.isna(row['성명']):
                    continue

                # 청번을 정수로 변환 (실패시 None으로 설정)
                try:
                    agency_number = int(row['청번']) if '청번' in row and not pd.isna(row['청번']) else None
                except ValueError:
                    agency_number = None

                # 데이터 삽입 수행
                cursor.execute(query, (
                    year, semester,  # 연도, 학기
                    row.get('청명') if not pd.isna(row.get('청명', pd.NA)) else None,
                    row.get('소속') if not pd.isna(row.get('소속', pd.NA)) else None,
                    agency_number,  # 정수로 변환된 청번
                    ele_school,  # 초등학교 여부
                    mid_school,  # 중학교 여부
                    row.get('성명') if not pd.isna(row.get('성명', pd.NA)) else None,
                    row.get('나이스 개인번호') if not pd.isna(row.get('나이스 개인번호', pd.NA)) else None,
                    row.get('생년월일-성별') if not pd.isna(row.get('생년월일-성별', pd.NA)) else None,
                    row.get('영역') if not pd.isna(row.get('영역', pd.NA)) else None,
                    int(row['시작일']) if not pd.isna(row.get('시작일', pd.NA)) else None,
                    int(row['종료일']) if not pd.isna(row.get('종료일', pd.NA)) else None,
                    datetime.now(),  # 현재 시간을 등록일자로 사용
                    row.get('비고') if not pd.isna(row.get('비고', pd.NA)) else None
                ))
            conn.commit()  # 모든 삽입이 성공하면 커밋
        except mariadb.Error as e:
            print(f"Error: {e}")
            conn.rollback()  # 오류 발생 시 롤백

    def process(self):
        """
        엑셀 파일을 처리하는 메인 메서드
        
        엑셀 파일의 각 시트('초'/'중')에 대해:
        1. 시트 첫 셀에서 연도와 학기 정보를 추출
        2. 데이터를 읽어서 DB에 삽입
        3. 작업 완료 후 연결 종료
        
        각 단계에서 발생하는 오류를 처리하고 적절한 메시지를 출력합니다.
        """
        try:
            excel_file = pd.ExcelFile(self.excel_file_path, engine='openpyxl')
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            return

        for sheet_name in excel_file.sheet_names:
            # 첫 번째 셀에서 연도와 학기 추출을 위해 데이터 읽기
            data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', header=None)
            sheet_header = str(data.iloc[0, 0])

            try:
                year, semester = self.extract_year_semester(sheet_header)
            except ValueError as e:
                print(f"Error: {e}")
                continue

            # 실제 데이터 읽기 (5번째 행부터)
            data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', skiprows=4)
            data.columns = ['연번', '청번', '청명', '소속', '성명', '나이스 개인번호',
                            '생년월일-성별', '영역', '시작일', '종료일', '비고']

            # 학교 구분 설정
            ele_school = '초' if sheet_name == '초' else None
            mid_school = '중' if sheet_name == '중' else None

            # DB 연결 및 데이터 삽입
            conn = self.connect()
            self.insert_data(conn, data, year, semester, ele_school, mid_school)
            conn.close()
            print(f"Data insertion complete for sheet '{sheet_name}' and connection closed.")

