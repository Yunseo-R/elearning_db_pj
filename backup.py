import mariadb
import pandas as pd  # 데이터 처리를 위한 pandas 라이브러리 (DataFrame 사용)
import sys
from datetime import datetime
import os # 파일과 폴더 관리를 위한 라이브러리

def connect():
    """
    데이터베이스 연결을 설정하고 연결 객체를 반환하는 함수
    conn: MariaDB 연결 객체
    """
    try:
        conn = mariadb.connect(
            user='ielearning',  # DB 접속 계정명
            password='pelearning!2345',  # DB 접속 비밀번호
            host='127.0.0.1',  # DB 서버 주소 (localhost)
            port=3306,  # DB 서버 포트
            database='elearning_db'  # 사용할 DB 이름
        )
    except mariadb.Error as e:
        # 연결 실패시 에러 메시지를 출력하고 프로그램 종료
        print(f"마리아DB와 연결 실패: {e}")
        sys.exit(1)
    return conn

def set_filename(base_path, current_date):
    """
    백업 파일명 기본값이 backup_YYYYMMDD.csv로 날짜이므로,
    파일명이 중복될 경우 뒤에 _1, _2와 같이 숫자를 붙여 구분해주는 함수
    Args:
        base_path (str): 백업 폴더 경로
        current_date (str): 현재 날짜 문자열 (YYYYMMDD 형식)
    Returns:
        str: 중복되지 않는 파일 경로

    동작 방식:
    1. 먼저 기본 파일명(backup_YYYYMMDD.csv)을 시도
    2. 이미 존재하면 번호를 붙여서(backup_YYYYMMDD_1.csv) 시도
    3. 그것도 존재하면 번호를 1씩 증가시키며 시도
    4. 존재하지 않는 파일명을 찾으면 그 이름을 반환
    """
    # 기본 파일명 형식
    base_filename = f"{base_path}/backup_{current_date}.csv"

    # 기본 파일명이 존재하지 않으면 그대로 사용
    if not os.path.exists(base_filename):
        return base_filename

    # 기본 파일명이 이미 존재하면 번호를 붙여가며 시도
    counter = 1
    while True:
        new_filename = f"{base_path}/backup_{current_date}_{counter}.csv"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1

def backup_database():
    """
    데이터베이스의 데이터를 CSV 파일로 백업하는 함수
    백업 파일은 'backup' 폴더에 'backup_YYYYMMDD.csv(_N)'형태로 저장됨
    """
    try:
        # 데이터베이스 연결
        conn = connect()  # DB 연결 객체 생성
        cursor = conn.cursor()  # SQL 쿼리 실행을 위한 커서 객체 생성

        # TB_ELEARNING_DATA 테이블의 모든 데이터를 조회하는 SQL 쿼리 실행
        cursor.execute("""
            SELECT year, semester, agency_name, affiliation, agency_number, 
                   ele_school, mid_school, t_name, nice_number, birthday,
                   area, start_date, end_date, reg_date, remarks
            FROM TB_ELEARNING_DATA
        """)

        # 쿼리 결과를 DataFrame으로 변환
        columns = [desc[0] for desc in cursor.description]  # 테이블의 컬럼명들을 가져옴
        data = pd.DataFrame(cursor.fetchall(), columns=columns)  # 조회된 데이터로 DataFrame 생성

        # 현재 날짜를 YYYYMMDD 형식의 문자열로 변환 (예: 20241127)
        current_date = datetime.now().strftime("%Y%m%d")
        backup_dir = "backup"  # 백업 파일을 저장할 폴더명

        # backup 폴더가 없으면 새로 생성
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        # 중복되지 않는 파일명 얻기
        backup_file = set_filename(backup_dir, current_date)

        # DataFrame을 CSV 파일로 저장
        # index=False: DataFrame의 인덱스는 저장하지 않음
        # encoding='utf-8-sig': 한글이 깨지지 않도록 UTF-8 인코딩 사용
        data.to_csv(backup_file, index=False, encoding='utf-8-sig')

        # 백업 완료 메시지 출력
        print(f"백업이 성공적으로 완료되었습니다.")
        print(f"백업 파일 위치: {backup_file}")

        # 데이터베이스 연결 종료
        conn.close()

    except mariadb.Error as e:
        # 데이터베이스 관련 오류 처리
        print(f"데이터베이스 오류 발생: {e}")
    except Exception as e:
        # 기타 모든 오류 처리
        print(f"백업 중 오류 발생: {e}")

def main():
    # 메인: 프로그램 시작점
    print("\n=== 데이터베이스 백업 프로그램 ===")
    print("데이터베이스 백업을 시작합니다...")
    backup_database()  # 백업DB 함수 실행

if __name__ == "__main__":
    main()
