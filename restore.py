import os
import pandas as pd
from datetime import datetime
from db_connector import DBConnector

class DatabaseRestore(DBConnector):
    """
    elearning_db 데이터베이스의 데이터를 CSV 파일을 사용해 복구하고,
    복구 이력을 관리하는 클래스.
    """

    def __init__(self):
        """DatabaseRestore 클래스 초기화"""
        super().__init__()

    def restore_from_csv(self, file_path: str) -> bool:
        """
        CSV 파일을 사용하여 tb_elearning_data 테이블을 복구하는 메서드.

        Args:
            file_path (str): 복구에 사용할 CSV 파일 경로.

        Returns:
            bool: 복구 성공 여부. True면 성공, False면 실패.
        """
        status = 0  # 초기 상태: 처리중
        try:
            # CSV 파일 읽기
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"지정된 파일을 찾을 수 없습니다: {file_path}")

            data = pd.read_csv(file_path, encoding='utf-8-sig')

            # 데이터베이스 연결
            conn = self.connect()
            cursor = conn.cursor()

            # tb_elearning_data의 기존 데이터 삭제
            cursor.execute("DELETE FROM tb_elearning_data")
            cursor.execute("ALTER TABLE tb_elearning_data AUTO_INCREMENT = 1")  # Auto Increment 초기화

            # 데이터 삽입 쿼리
            query = ("INSERT INTO tb_elearning_data (year, semester, agency_name, affiliation, "
                     "agency_number, school_type, t_name, nice_number, birthday, "
                     "area, start_date, end_date, reg_date, remarks) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            # 데이터 삽입
            for _, row in data.iterrows():
                # None 또는 NaN 값을 처리하여 데이터베이스에 NULL로 저장
                cursor.execute(query, (
                    row['year'] if not pd.isna(row['year']) else None,
                    row['semester'] if not pd.isna(row['semester']) else None,
                    row['agency_name'] if not pd.isna(row['agency_name']) else None,
                    row['affiliation'] if not pd.isna(row['affiliation']) else None,
                    row['agency_number'] if not pd.isna(row['agency_number']) else None,
                    row['school_type'] if not pd.isna(row['school_type']) else None,
                    row['t_name'] if not pd.isna(row['t_name']) else None,
                    row['nice_number'] if not pd.isna(row['nice_number']) else None,
                    row['birthday'] if not pd.isna(row['birthday']) else None,
                    row['area'] if not pd.isna(row['area']) else None,
                    row['start_date'] if not pd.isna(row['start_date']) else None,
                    row['end_date'] if not pd.isna(row['end_date']) else None,
                    row['reg_date'] if not pd.isna(row['reg_date']) else None,
                    row['remarks'] if not pd.isna(row['remarks']) else None
                ))
            conn.commit()

            # 복구 성공 상태로 로그 저장
            status = 1
            self.save_restore_log(file_path, status)
            print(f"복구가 성공적으로 완료되었습니다. 파일 경로: {file_path}")

            conn.close()
            return True
        except Exception as e:
            print(f"복구 중 오류 발생: {e}")

            # 복구 실패 상태로 로그 저장
            status = 2
            self.save_restore_log(file_path, status)

            if 'conn' in locals():
                conn.close()
            return False

    def save_restore_log(self, file_path: str, status: int):
        """
        tb_restore_data 테이블에 복구 정보를 저장하는 메서드.

        Args:
            file_path (str): 복구에 사용한 CSV 파일 경로.
            status (int): 복구 상태 (0: 대기 또는 처리중, 1: 복구 완료, 2: 복구 실패).
        """
        try:
            # 데이터베이스 연결
            conn = self.connect()
            cursor = conn.cursor()

            # 현재 시간 가져오기
            current_time = datetime.now()

            # tb_restore_data 테이블에 복구 정보 삽입
            cursor.execute("""
                INSERT INTO tb_restore_data (restore_date, file_name, status)
                VALUES (?, ?, ?)
            """, (current_time, os.path.basename(file_path), status))
            conn.commit()

            conn.close()
            print(f"복구 정보가 저장되었습니다. 파일명: {os.path.basename(file_path)}, 상태: {status}")
        except Exception as e:
            print(f"복구 정보 저장 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()

    def fetch_restore_history(self):
        """
        tb_restore_data 테이블에서 복구 이력을 조회하는 메서드.

        Returns:
            List[Dict]: 복구 이력 목록. 각 이력은 딕셔너리 형태로 반환.
                - dat_idx (int): 복구 데이터 ID.
                - restore_date (datetime): 복구 실행 날짜 및 시간.
                - file_name (str): 복구에 사용된 파일 이름.
                - status (str): 복구 상태 ('대기', '복구 완료', '복구 실패').
        """
        try:
            # 데이터베이스 연결
            conn = self.connect()
            cursor = conn.cursor()

            # 복구 이력 조회
            cursor.execute("""
                SELECT dat_idx, restore_date, file_name, status
                FROM tb_restore_data
                ORDER BY restore_date DESC
            """)
            columns = [desc[0] for desc in cursor.description]

            # 상태값 변환 및 데이터 정리
            results = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                if record['status'] == 0:
                    record['status'] = "대기"
                elif record['status'] == 1:
                    record['status'] = "복구 완료"
                elif record['status'] == 2:
                    record['status'] = "복구 실패"
                results.append(record)

            conn.close()
            return results
        except Exception as e:
            print(f"복구 이력 조회 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []
