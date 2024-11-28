import os
from upload import DataImporter
from backup import DatabaseBackup
from restore import DatabaseRestore
from db_search import DataSearcher

class ELearningSystem:
    """E-Learning 시스템의 메인 클래스"""

    def __init__(self):
        """
        클래스 초기화
        menu_options: 메뉴 번호와 해당 기능을 매핑한 딕셔너리
        searcher: 데이터 검색을 위한 DataSearcher 클래스의 인스턴스
        """
        self.menu_options = {
            '1': ('데이터 업로드', self.run_upload),
            '2': ('데이터베이스 백업', self.run_backup),
            '3': ('데이터베이스 복원', self.run_restore),
            '4': ('데이터 검색', self.run_search),
            '5': ('종료', self.exit_program)
        }
        self.searcher = DataSearcher()

    def display_menu(self):
        """메인 메뉴 표시"""
        print("\n=== E-Learning 데이터 관리 시스템 ===")
        print("원하시는 작업을 선택해주세요:")
        for key, (option, _) in self.menu_options.items():
            print(f"{key}. {option}")

    def get_valid_file_path(self, prompt: str) -> str:
        """
        유효한 파일 경로를 입력받는 함수
        Args:
            prompt (str): 사용자에게 보여줄 입력 메시지
        Returns:
            str: 유효한 파일의 절대 경로 또는 None
        """
        while True:
            try:
                file_path = input(prompt).strip().strip('"').strip("'")
                abs_path = os.path.abspath(file_path)

                if os.path.exists(abs_path):
                    return abs_path
                else:
                    print(f"Error: 파일을 찾을 수 없습니다 - {abs_path}")
                    retry = input("다시 입력하시겠습니까? (y/n): ").lower().strip()
                    if retry != 'y':
                        return None
            except Exception as e:
                print(f"Error: 잘못된 파일 경로입니다 - {str(e)}")
                retry = input("다시 입력하시겠습니까? (y/n): ").lower().strip()
                if retry != 'y':
                    return None

    def run_upload(self):
        """데이터 업로드 기능 실행"""
        print("\n=== 데이터 업로드 ===")
        excel_path = self.get_valid_file_path("엑셀 파일 경로를 입력하세요: ")

        if not excel_path:
            print("데이터 업로드가 취소되었습니다.")
            return

        try:
            importer = DataImporter(excel_path)
            importer.process()
            print("데이터 업로드가 완료되었습니다.")
        except Exception as e:
            print(f"데이터 업로드 중 오류 발생: {e}")

    def get_valid_directory(self, prompt: str, create_if_missing: bool = True) -> str:
        """
        유효한 디렉토리 경로를 입력받는 함수
        Args:
            prompt (str): 사용자에게 보여줄 입력 메시지
            create_if_missing (bool): 디렉토리가 없을 경우 생성 여부
        Returns:
            str: 유효한 디렉토리의 절대 경로 또는 None
        """
        while True:
            dir_path = input(prompt).strip().strip('"').strip("'")

            if not dir_path:
                dir_path = 'backup'

            abs_path = os.path.abspath(dir_path)

            if not os.path.exists(abs_path):
                if create_if_missing:
                    try:
                        os.makedirs(abs_path)
                        print(f"디렉토리가 생성되었습니다: {abs_path}")
                        return abs_path
                    except Exception as e:
                        print(f"Error: 디렉토리 생성 실패 - {str(e)}")
                        retry = input("다시 입력하시겠습니까? (y/n): ").lower().strip()
                        if retry != 'y':
                            return None
                else:
                    print(f"Error: 디렉토리를 찾을 수 없습니다 - {abs_path}")
                    retry = input("다시 입력하시겠습니까? (y/n): ").lower().strip()
                    if retry != 'y':
                        return None
            else:
                return abs_path

    def run_backup(self):
        """데이터베이스 백업 기능 실행"""
        print("\n=== 데이터베이스 백업 ===")
        backup_dir = self.get_valid_directory("백업 폴더 경로를 입력하세요 (기본값: 'backup'): ")

        if not backup_dir:
            print("백업이 취소되었습니다.")
            return

        try:
            backup = DatabaseBackup(backup_dir)
            backup.backup()
        except Exception as e:
            print(f"백업 중 오류 발생: {e}")

    def run_restore(self):
        """데이터베이스 복원 기능 실행"""
        print("\n=== 데이터베이스 복원 ===")
        backup_dir = self.get_valid_directory("백업 폴더 경로를 입력하세요 (기본값: 'backup'): ", create_if_missing=False)

        if not backup_dir:
            print("복원이 취소되었습니다.")
            return

        file_pattern = input("파일명 패턴을 입력하세요 (모든 CSV 파일: Enter): ").strip()

        try:
            restore = DatabaseRestore(backup_dir)
            backup_files = restore.get_backup_files(file_pattern if file_pattern else None)

            if not backup_files:
                return

            print("\n사용 가능한 백업 파일:")
            for i, file in enumerate(backup_files, 1):
                print(f"{i}. {file}")

            while True:
                try:
                    choice = input("\n복원할 백업 파일 번호를 선택하세요 (취소: q): ").strip()
                    if choice.lower() == 'q':
                        print("복원이 취소되었습니다.")
                        break

                    choice = int(choice)
                    if 1 <= choice <= len(backup_files):
                        selected_file = backup_files[choice-1]
                        restore.restore(selected_file)
                        break
                    else:
                        print("잘못된 번호입니다. 다시 선택해주세요.")
                except ValueError:
                    print("올바른 숫자를 입력하거나 'q'를 입력하여 취소하세요.")

        except Exception as e:
            print(f"복원 중 오류 발생: {e}")

    def run_search(self):
        """데이터 검색 기능 실행"""
        print("\n=== 데이터 검색 ===")

        # 연도/학기 목록 조회
        years_semesters = self.searcher.get_available_years_and_semesters()
        if not years_semesters:
            print("데이터베이스에 검색 가능한 데이터가 없습니다.")
            return

        # 연도 선택
        print("\n연도 선택:")
        available_years = sorted(set(year for year, _ in years_semesters), reverse=True)
        for i, year in enumerate(available_years, 1):
            print(f"{i}. {year}년")

        year_idx = -1
        while year_idx < 0:
            try:
                choice = input("\n연도를 선택하세요: ").strip()
                year_idx = int(choice) - 1
                if not (0 <= year_idx < len(available_years)):
                    print("잘못된 선택입니다.")
                    year_idx = -1
            except ValueError:
                print("올바른 숫자를 입력하세요.")

        selected_year = available_years[year_idx]

        # 선택된 연도의 학기 목록 조회
        print("\n학기 선택:")
        available_semesters = sorted(set(sem for year, sem in years_semesters if year == selected_year))
        for i, semester in enumerate(available_semesters, 1):
            print(f"{i}. {semester}학기")

        semester_idx = -1
        while semester_idx < 0:
            try:
                choice = input("\n학기를 선택하세요: ").strip()
                semester_idx = int(choice) - 1
                if not (0 <= semester_idx < len(available_semesters)):
                    print("잘못된 선택입니다.")
                    semester_idx = -1
            except ValueError:
                print("올바른 숫자를 입력하세요.")

        selected_semester = available_semesters[semester_idx]

        # 검색 조건 입력
        print("\n검색 조건을 입력하세요 (입력하지 않으면 모든 데이터 조회):")
        name = input("이름: ").strip()
        birthday = input("생년월일 (예: 990203-1): ").strip()

        # 검색 실행
        if not name and not birthday:
            print("이름이나 생년월일 중 하나는 입력해야 합니다.")
            return

        results = self.searcher.search_by_name_or_birthday(
            selected_year, selected_semester,
            name=name if name else None,
            birthday=birthday if birthday else None
        )

        # 검색 결과 출력
        if not results:
            print("\n검색 결과가 없습니다.")
            return

        print(f"\n검색 결과: {len(results)}건")
        print("\n" + "=" * 100)
        for result in results:
            print(f"소속: {result['affiliation']}")
            print(f"청명: {result['agency_name']}")
            print(f"청번: {result['agency_number']}")
            print(f"성명: {result['t_name']}")
            print(f"생년월일: {result['birthday']}")
            print(f"나이스번호: {result['nice_number']}")
            print(f"영역: {result['area']}")
            print(f"학교구분: {'초등' if result['ele_school'] else '중등' if result['mid_school'] else '미지정'}")
            print(f"교육기간: {result['start_date']} ~ {result['end_date']}")
            print("-" * 100)

    def run(self):
        """
        메인 프로그램 실행
        사용자가 종료를 선택할 때까지 메뉴를 반복해서 표시하고
        선택된 작업을 수행합니다.
        """
        while True:
            try:
                self.display_menu()
                choice = input("\n선택: ").strip()

                if choice in self.menu_options:
                    _, func = self.menu_options[choice]
                    func()
                else:
                    # 사용자가 파일 경로를 직접 입력한 경우
                    if os.path.exists(choice):
                        self.menu_options['1'][1]()  # 업로드 기능 실행
                    else:
                        print("잘못된 선택입니다. 1-5 사이의 숫자를 입력해주세요.")
            except Exception as e:
                print(f"Error: {str(e)}")
                print("프로그램을 계속 실행합니다.")

    def exit_program(self):
        """프로그램 종료"""
        print("\n프로그램을 종료합니다.")
        exit()

def main():
    """프로그램의 시작점"""
    system = ELearningSystem()
    system.run()

if __name__ == "__main__":
    main()
