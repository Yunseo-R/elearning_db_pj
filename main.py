import mariadb
import pandas as pd
import sys
from datetime import datetime
import re

# 사용하려면 터미널에 pip install mariadb pandas openpyxl 를 입력해 라이브러리를 설치하세요

# 마리아DB와 연결 설정
def connect():
    try:
        conn = mariadb.connect(
            user='ielearning', # 사용자명
            password='pelearning!2345', # 사용자 비밀번호
            host='127.0.0.1', # 호스트 주소(현재 localhost 주소)
            port=3306, # 포트 번호
            database='elearning_db' # 연결할 데이터베이스 이름
        )
    except mariadb.Error as e:
        print(f"마리아DB와 연결 실패: {e}")
        sys.exit(1) # 연결 실패 시 프로그램 종료
    return conn

# 엑셀 시트의 첫번째 셀을 제목으로 간주하고 제목에서 연도와 학기를 추출하는 함수
def extract_year_semester(sheet_header):
    # 제목에서 연도와 학기 추출을 위한 표현식
    year_match = re.search(r'(\d{4})학년도', sheet_header)
    semester_match = re.search(r'(\d)학기', sheet_header)

    # 추출 실패 시 에러 로그
    if not year_match:
        raise ValueError("시트 제목에서 연도를 추출할 수 없습니다.")
    if not semester_match:
        raise ValueError("시트 제목에서 학기를 추출할 수 없습니다.")

    # 추출한 연도와 학기 정보를 int로 변환
    year = int(year_match.group(1))
    semester = int(semester_match.group(1))

    return year, semester

# DB에 데이터를 삽입
def insert_data(conn, data, year, semester, ele_school, mid_school):
    cursor = conn.cursor()
    query = ("INSERT INTO TB_ELEARNING_DATA (year, semester, agency_name, affiliation, agency_number, ele_school, mid_school, t_name, nice_number, birthday, area, start_date, end_date, reg_date, remarks) "
             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    try:
        for _, row in data.iterrows():
            # 한 행의 소속과 이름이 모두 빈칸인 경우 해당 행 스킵
            # 연번만 존재하는 빈 행이 null 형태로 DB에 삽입되는 것을 막기 위함
            if pd.isna(row['소속']) and pd.isna(row['성명']):
                continue

            try:
                # 청번을 정수로 변환. 변환 실패시 None
                agency_number = int(row['청번']) if '청번' in row and not pd.isna(row['청번']) else None
            except ValueError:
                agency_number = None

            # 쿼리 실행
            cursor.execute(query, (
                year, # 제목에서 추출한 연도
                semester, # 제목에서 추출한 학기
                row['청명'] if '청명' in row and not pd.isna(row['청명']) else None,
                row['소속'] if '소속' in row and not pd.isna(row['소속']) else None,
                agency_number, # 기관번호
                ele_school, # 시트명이 '초'인 경우 '초' / 아닌 경우 null
                mid_school, # 시트명이 '중'인 경우 '중' / 아닌 경우 null
                row['성명'] if '성명' in row and not pd.isna(row['성명']) else None,
                row['나이스 개인번호'] if '나이스 개인번호' in row and not pd.isna(row['나이스 개인번호']) else None,
                row['생년월일-성별'] if '생년월일-성별' in row and not pd.isna(row['생년월일-성별']) else None,
                row['영역'] if '영역' in row and not pd.isna(row['영역']) else None,
                int(row['시작일']) if '시작일' in row and not pd.isna(row['시작일']) else None,
                int(row['종료일']) if '종료일' in row and not pd.isna(row['종료일']) else None,
                datetime.now(), # 등록일
                row['비고'] if '비고' in row and not pd.isna(row['비고']) else None
            ))
        conn.commit() # 변경사항 커밋
    except mariadb.Error as e:
        print(f"Error: {e}")
        conn.rollback() #오류 발생 시 롤백

def main():
    # 엑셀 파일 경로
    excel_file_path = "C:\\Users\\huintech\\Desktop\\test.xlsx"

    # 엑셀 파일 읽기
    try:
        excel_file = pd.ExcelFile(excel_file_path, engine='openpyxl') # 불러오기
    except FileNotFoundError:
        print("Error: The specified file was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        sys.exit(1)

    # 각 시트에 대한 데이터 처리
    for sheet_name in excel_file.sheet_names:
        # 시트의 첫 번째 셀에서 year와 semester 추출
        data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', header=None) # 시트 읽어오기
        sheet_header = str(data.iloc[0, 0])  # 첫 번째 셀의 값을 시트 제목으로 사용
        try:
            year, semester = extract_year_semester(sheet_header) # 추출
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        # 실제 데이터는 다섯 번째 행 이후부터 시작
        data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', skiprows=4)

        # 엑셀 파일의 데이터 프레임 열 이름을 특정 형식으로 설정하여 데이터베이스와 일치시키기 위해 열 이름을 변경
        data.columns = ['연번', '청번', '청명', '소속', '성명', '나이스 개인번호', '생년월일-성별', '영역', '시작일', '종료일', '비고']

        # ele_school과 mid_school 설정
        ele_school = '초' if sheet_name == '초' else None
        mid_school = '중' if sheet_name == '중' else None

        # DB 연결
        conn = connect()

        # 데이터 삽입
        insert_data(conn, data, year, semester, ele_school, mid_school)

        # 연결 종료
        conn.close()
        print(f"Data insertion complete for sheet '{sheet_name}' and connection closed.")

# 메인 함수 실행
if __name__ == "__main__":
    main()
