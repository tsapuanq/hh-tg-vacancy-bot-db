import logging
from src.llm_summary import summarize_description_llm


class CompanyCache:


    def __init__(self, db):
        self.db = db
        self.cache = {}
        self._load_cache_from_db()

    def _load_cache_from_db(self):
        """Загружает все компании с валидным summary в память (dict)."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT company_name, summary_company
                FROM public.company_summaries
                WHERE summary_company IS NOT NULL
                  AND TRIM(summary_company) <> ''
                  AND LOWER(TRIM(summary_company)) NOT IN ('не указано', 'нет данных')
            """)
            rows = cursor.fetchall()
            self.cache = {r[0].strip(): r[1].strip() for r in rows if r[0] and r[1]}
            logging.info(f"🧠 CompanyCache: загружено {len(self.cache)} компаний из БД.")
        except Exception as e:
            logging.error(f"❌ Ошибка при загрузке company_summaries: {e}")
        finally:
            cursor.close()
            self.db.return_connection(conn)

    def get_summary(self, company_name, description=None, existing_summary=None):
        """
        Возвращает summary компании:
        - если уже есть в кэше → берёт из памяти
        - если передано existing_summary → использует его
        - иначе вызывает LLM
        """
        if not company_name or not company_name.strip():
            return "Не указано"

        company_name = company_name.strip()

        # 1️⃣ Проверяем кэш
        if company_name in self.cache:
            logging.info(f"♻️ Кэш найден для компании: {company_name}")
            return self.cache[company_name]

        # 2️⃣ Если LLM уже вернул summary — используем его
        if existing_summary:
            summary_text = str(existing_summary).strip()
        else:
            summary_data = summarize_description_llm(description or "")
            summary_text = str(summary_data.get("about_company") or "Не указано").strip()

        # 3️⃣ Сохраняем в кэш и в БД (если валидно)
        if summary_text and summary_text.lower() not in ("не указано", "нет данных"):
            self.cache[company_name] = summary_text
            self._save_to_db(company_name, summary_text)
            logging.info(f"💾 Добавлено в кэш: {company_name}")
        else:
            logging.info(f"⚠️ Пустой summary для компании: {company_name}")

        return summary_text or "Не указано"

    def _save_to_db(self, company_name, summary_text):
        """Сохраняет summary компании в таблицу company_summaries."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO public.company_summaries (company_name, summary_company, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (company_name)
                DO UPDATE SET summary_company = EXCLUDED.summary_company, updated_at = NOW();
            """, (company_name, summary_text))
            conn.commit()
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении summary для {company_name}: {e}")
        finally:
            cursor.close()
            self.db.return_connection(conn)
