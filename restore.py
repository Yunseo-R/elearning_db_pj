# 필요한 라이브러리들을 가져옵니다
import mariadb  # MariaDB 데이터베이스 연결을 위한 라이브러리
import pandas as pd  # 데이터 처리를 위한 pandas 라이브러리 (DataFrame 사용)
import sys  # 시스템 관련 기능(프로그램 종료 등)을 위한 라이브러리
from datetime import datetime  # 날짜와 시간 처리를 위한 라이브러리
import os  # 파일과 폴더 관리를 위한 라이브러리

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

def get_backup_files(backup_dir="backup", file_pattern=None):
    """
    지정된 폴더에서 CSV 파일 목록을 검색해 반환하는 함수
    Args:
        backup_dir (str): 백업 파일이 저장된 폴더 경로 (기본값: 'backup')
        file_pattern (str): 검색할 파일명 패턴 (기본값: None, 모든 CSV 파일 검색)
    Returns:
        list: CSV 파일명들의 리스트 (최신 파일이 먼저 오도록 정렬됨)
    """
    # 백업 폴더가 없으면 빈 리스트 반환
    if not os.path.exists(backup_dir):
        print(f"백업 폴더({backup_dir})가 존재하지 않습니다.")
        return []

    # 모든 파일 목록 가져오기
    all_files = os.listdir(backup_dir)

    # CSV 파일 필터링
    if file_pattern:
        # 특정 패턴으로 시작하는 CSV 파일만 선택
        backup_files = [f for f in all_files if f.startswith(file_pattern) and f.endswith('.csv')]
    else:
        # 모든 CSV 파일 선택
        backup_files = [f for f in all_files if f.endswith('.csv')]

    # 파일이 없는 경우 처리
    if not backup_files:
        pattern_msg = f"패턴 '{file_pattern}'과 일치하는 " if file_pattern else ""
        print(f"폴더에 {pattern_msg}CSV 파일이 없습니다.")
        return []

    return sorted(backup_files, reverse=True)  # 최신 파일이 위에 오도록 정렬

def restore_database(backup_file, backup_dir="backup"):
    """
    선택된 백업 파일의 데이터로 데이터베이스를 복구하는 함수
    
    Args:
        backup_file (str): 복구에 사용할 백업 파일명
        backup_dir (str): 백업 파일이 저장된 폴더 경로 (기본값: 'backup')
    """
    try:
        # 백업 파일의 전체 경로 생성
        backup_path = os.path.join(backup_dir, backup_file)

        # 파일 존재 여부 확인
        if not os.path.exists(backup_path):
            print(f"오류: 파일을 찾을 수 없습니다 - {backup_path}")
            return

        # CSV 파일을 DataFrame으로 읽어오기
        print(f"백업 파일을 읽는 중: {backup_file}")
        try:
            data = pd.read_csv(backup_path)
        except Exception as e:
            print(f"파일 읽기 실패: {e}")
            return

        # 필수 컬럼이 있는지 확인
        required_columns = ['year', 'semester', 'agency_name', 'affiliation', 'agency_number',
                            'ele_school', 'mid_school', 't_name', 'nice_number', 'birthday',
                            'area', 'start_date', 'end_date', 'reg_date', 'remarks']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            print(f"오류: 필수 컬럼이 누락되었습니다 - {', '.join(missing_columns)}")
            return

        # 데이터베이스 연결
        conn = connect()
        cursor = conn.cursor()

        # 복구 작업 시작 전 사용자 확인
        print("\n경고: 이 작업은 현재 데이터베이스의 모든 데이터를 삭제하고 백업 데이터로 교체합니다.")
        confirm = input("계속 진행하시겠습니까? (y/n): ")
        if confirm.lower() != 'y':
            print("복구 작업이 취소되었습니다.")
            conn.close()
            return

        try:
            # 테이블 초기화 (기존 데이터 삭제 및 자동 증가 값 초기화)
            print("기존 데이터를 삭제하고 AUTO_INCREMENT를 초기화하는 중...")
            cursor.execute("TRUNCATE TABLE TB_ELEARNING_DATA")

            # 백업 데이터 삽입
            print("백업 데이터를 복구하는 중...")
            # SQL 삽입 쿼리 준비
            insert_query = """
                INSERT INTO TB_ELEARNING_DATA 
                (year, semester, agency_name, affiliation, agency_number, 
                 ele_school, mid_school, t_name, nice_number, birthday,
                 area, start_date, end_date, reg_date, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # DataFrame의 각 행을 순회하며 데이터 삽입
            for _, row in data.iterrows():
                values = []
                for value in row:
                    # NULL 값 처리
                    if pd.isna(value):
                        values.append(None)
                    else:
                        # reg_date 컬럼의 경우 문자열을 datetime 객체로 변환
                        if isinstance(value, str) and 'reg_date' in data.columns[len(values)]:
                            try:
                                values.append(datetime.strptime(value, '%Y-%m-%d %H:%M:%S'))
                            except ValueError:
                                values.append(None)
                        else:
                            values.append(value)

                # 데이터 삽입 실행
                cursor.execute(insert_query, values)

            # 모든 변경사항 저장
            conn.commit()
            print(f"\n복구가 성공적으로 완료되었습니다.")
            print(f"복구된 레코드 수: {len(data)}")

        except Exception as e:
            # 오류 발생 시 모든 변경사항 취소
            conn.rollback()
            print(f"복구 중 오류 발생. 변경사항이 취소되었습니다: {e}")

        finally:
            # 데이터베이스 연결 종료
            conn.close()

    except Exception as e:
        print(f"복구 중 오류 발생: {e}")

def main():
    """
    프로그램의 메인 함수
    사용자에게 복구 가능한 백업 파일 목록을 보여주고,
    선택한 파일로 복구를 진행합니다.
    """
    print("\n=== 데이터베이스 복구 프로그램 ===")

    # 기본 백업 폴더 설정
    default_backup_dir = "backup"

    # 사용자 정의 백업 폴더 입력 받기 (선택사항)
    user_backup_dir = input("백업 폴더 경로를 입력하세요 (기본값: 'backup'): ").strip()
    backup_dir = user_backup_dir if user_backup_dir else default_backup_dir

    # 파일명 패턴 입력 받기 (선택사항)
    file_pattern = input("파일명 패턴을 입력하세요 (모든 CSV 파일: Enter): ").strip()

    # 사용 가능한 백업 파일 목록 가져오기
    backup_files = get_backup_files(backup_dir, file_pattern if file_pattern else None)

    # 백업 파일이 없으면 프로그램 종료
    if not backup_files:
        return

    # 백업 파일 목록 출력
    print("\n사용 가능한 백업 파일:")
    for i, file in enumerate(backup_files, 1):
        print(f"{i}. {file}")

    # 사용자로부터 복구할 파일 선택 받기
    while True:
        try:
            choice = int(input("\n복구할 백업 파일 번호를 선택하세요: "))
            if 1 <= choice <= len(backup_files):
                selected_file = backup_files[choice-1]
                break
            else:
                print("잘못된 번호입니다. 다시 선택해주세요.")
        except ValueError:
            print("숫자를 입력해주세요.")

    # 선택된 파일로 복구 실행
    restore_database(selected_file, backup_dir)

# 프로그램 시작점
if __name__ == "__main__":
    main()
