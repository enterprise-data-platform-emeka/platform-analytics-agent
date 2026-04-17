"""Analytics Agent Streamlit UI.

Calls the FastAPI backend at localhost:8080. Both processes run in the same
ECS Fargate container, started by entrypoint.sh after FastAPI is healthy.

Stakeholders open the ALB DNS address on port 8501 in a browser. All AWS API
calls happen server-side in the container — the browser never touches AWS.
"""

import base64 as _b64
import glob as _glob
import html as html_lib
import json
import os
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

BACKEND_URL = "http://localhost:8080"
_BERLIN = ZoneInfo("Europe/Berlin")

# Fallback example questions used when /examples endpoint is unreachable.
_FALLBACK_EXAMPLES = [
    "Which country has the highest total revenue?",
    "What are the top 10 best-selling products by revenue?",
    "Which carrier has the fastest average delivery time?",
    "Show me monthly revenue trends for the last year.",
]

# Rephrasing tips (English). Translated versions live in _UI_TRANSLATIONS["rephrase_tips"].
_REPHRASE_TIPS = (
    "**Tips for rephrasing:**\n"
    "- Be specific about time periods: *'in 2024'*, *'last 6 months'*\n"
    "- Ask about one metric at a time: revenue, orders, delivery time\n"
    "- Use table-level language: *'by country'*, *'per carrier'*, *'by product'*\n"
    "- Avoid open-ended questions — the Gold tables are pre-aggregated summaries"
)

# ── Language detection and UI translations ─────────────────────────────────────


def _detect_language(text: str) -> str:
    """Detect the dominant script from Unicode block ranges.

    Returns a language code ('zh', 'ja', 'ko', 'ar', 'ru', 'el', 'he', 'th')
    or 'en' when only Latin/ASCII characters are found.
    """
    counts: dict[str, int] = {}
    for ch in text:
        cp = ord(ch)
        if 0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF:
            counts["zh"] = counts.get("zh", 0) + 1
        elif 0x3040 <= cp <= 0x309F or 0x30A0 <= cp <= 0x30FF:
            counts["ja"] = counts.get("ja", 0) + 1
        elif 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF:
            counts["ko"] = counts.get("ko", 0) + 1
        elif 0x0600 <= cp <= 0x06FF:
            counts["ar"] = counts.get("ar", 0) + 1
        elif 0x0400 <= cp <= 0x04FF:
            counts["ru"] = counts.get("ru", 0) + 1
        elif 0x0370 <= cp <= 0x03FF:
            counts["el"] = counts.get("el", 0) + 1
        elif 0x0590 <= cp <= 0x05FF:
            counts["he"] = counts.get("he", 0) + 1
        elif 0x0E00 <= cp <= 0x0E7F:
            counts["th"] = counts.get("th", 0) + 1
    if not counts:
        return "en"
    return max(counts, key=lambda k: counts[k])


_UI_TRANSLATIONS: dict[str, dict[str, str]] = {
    "zh": {
        # Turn card
        "Question": "问题",
        # Answer labels
        "Chart": "图表",
        "Table": "数据表",
        "Details": "详细信息",
        "Assumptions": "假设条件",
        "Query intent check": "查询意图检查",
        "Claude was shown only the SQL (not your question) and asked what it thinks the query is trying to answer:": "Claude 仅看到 SQL（未看到您的问题），并被要求推断该查询试图回答什么：",
        "Inferred:": "推断：",
        "Data quality notice:": "数据质量提示：",
        "Tips for rephrasing your question": "重新表述问题的建议",
        # Action buttons
        "Download PDF": "下载 PDF",
        "Send as email": "发送至邮箱",
        "Close email": "关闭邮件",
        "Recipient email address": "收件人邮箱",
        "Send PDF report": "发送 PDF 报告",
        "Sending...": "发送中...",
        "Enter a recipient email address.": "请输入收件人邮箱地址。",
        # Metrics
        "Athena cost": "Athena 费用",
        "Data scanned": "扫描数据量",
        "Chart type": "图表类型",
        # Error messages
        "Could not answer:": "无法回答：",
        "Request timed out. The query may be complex — try again.": "请求超时，查询可能较为复杂，请重试。",
        "Agent error:": "代理错误：",
        "Could not reach backend:": "无法连接后端：",
        # PDF section headings
        "EDP Analytics Report": "EDP 数据分析报告",
        "Summary": "摘要",
        "SQL Query": "SQL 查询",
        "Query Metadata": "查询元数据",
        "Query Intent Check": "查询意图检查",
        "Athena cost:": "Athena 费用：",
        "Data scanned:": "扫描数据量：",
        "Chart type:": "图表类型：",
        # Sidebar
        "Session": "会话",
        "Started at": "开始于",
        "questions_answered_fmt": "已回答 {n} 个问题",
        "Start new session": "开始新会话",
        "This will clear all questions. Are you sure?": "这将清除所有问题，确定吗？",
        "Yes, clear": "是，清除",
        "Cancel": "取消",
        "Export conversation (JSON)": "导出对话 (JSON)",
        "Import conversation (JSON)": "导入对话 (JSON)",
        "History": "历史记录",
        "restored_fmt": "已恢复 {n} 条对话。",
        "Import failed:": "导入失败：",
        # Page header / empty state
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "用任何语言提问 Gold 层数据。后续问题会记住上下文。",
        "Try asking:": "试着问：",
        # Rephrase tips (full translated block)
        "rephrase_tips": (
            "**重新表述建议：**\n"
            "- 具体说明时间段：*'2024年'*，*'过去6个月'*\n"
            "- 每次只问一个指标：收入、订单数、配送时长\n"
            "- 使用维度语言：*'按国家'*、*'按承运商'*、*'按产品'*\n"
            "- 避免开放性问题——Gold 层是预聚合汇总数据"
        ),
    },
    "ja": {
        "Question": "質問",
        "Chart": "グラフ",
        "Table": "テーブル",
        "Details": "詳細",
        "Assumptions": "前提条件",
        "Query intent check": "クエリ意図確認",
        "Claude was shown only the SQL (not your question) and asked what it thinks the query is trying to answer:": "Claude は SQL のみを確認し（質問は非表示）、クエリが何を答えようとしているか推測しました：",
        "Inferred:": "推測：",
        "Data quality notice:": "データ品質通知：",
        "Tips for rephrasing your question": "質問の言い換えヒント",
        "Download PDF": "PDF をダウンロード",
        "Send as email": "メールで送信",
        "Close email": "メールを閉じる",
        "Recipient email address": "受信者メールアドレス",
        "Send PDF report": "PDF レポートを送信",
        "Sending...": "送信中...",
        "Enter a recipient email address.": "受信者のメールアドレスを入力してください。",
        "Athena cost": "Athena コスト",
        "Data scanned": "スキャンデータ量",
        "Chart type": "グラフタイプ",
        "Could not answer:": "回答できませんでした：",
        "Request timed out. The query may be complex — try again.": "リクエストがタイムアウトしました。再試行してください。",
        "Agent error:": "エージェントエラー：",
        "Could not reach backend:": "バックエンドに接続できませんでした：",
        "EDP Analytics Report": "EDP アナリティクスレポート",
        "Summary": "まとめ",
        "SQL Query": "SQL クエリ",
        "Query Metadata": "クエリメタデータ",
        "Query Intent Check": "クエリ意図確認",
        "Athena cost:": "Athena コスト：",
        "Data scanned:": "スキャンデータ量：",
        "Chart type:": "グラフタイプ：",
        "Session": "セッション",
        "Started at": "開始時刻",
        "questions_answered_fmt": "{n} 件の質問に回答済み",
        "Start new session": "新しいセッションを開始",
        "This will clear all questions. Are you sure?": "すべての質問が消去されます。よろしいですか？",
        "Yes, clear": "はい、消去する",
        "Cancel": "キャンセル",
        "Export conversation (JSON)": "会話をエクスポート (JSON)",
        "Import conversation (JSON)": "会話をインポート (JSON)",
        "History": "履歴",
        "restored_fmt": "{n} 件の会話を復元しました。",
        "Import failed:": "インポート失敗：",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Gold データについてどんな言語でも質問できます。フォローアップは前の文脈を記憶します。",
        "Try asking:": "試しに聞いてみてください：",
        "rephrase_tips": (
            "**言い換えのヒント：**\n"
            "- 期間を具体的に：*'2024年'*、*'過去6か月'*\n"
            "- 一度に1つの指標を質問：収益、注文数、配送日数\n"
            "- 集計軸を使う：*'国別'*、*'キャリア別'*、*'製品別'*\n"
            "- Gold テーブルは事前集計済みなのでオープンエンドな質問は避けてください"
        ),
    },
    "ko": {
        "Question": "질문",
        "Chart": "차트",
        "Table": "테이블",
        "Details": "세부 정보",
        "Assumptions": "가정 사항",
        "Query intent check": "쿼리 의도 확인",
        "Claude was shown only the SQL (not your question) and asked what it thinks the query is trying to answer:": "Claude 는 SQL만 확인하고 (질문 제외) 쿼리가 무엇을 답하려는지 추론했습니다：",
        "Inferred:": "추론：",
        "Data quality notice:": "데이터 품질 알림：",
        "Tips for rephrasing your question": "질문 재표현 팁",
        "Download PDF": "PDF 다운로드",
        "Send as email": "이메일로 전송",
        "Close email": "이메일 닫기",
        "Recipient email address": "수신자 이메일 주소",
        "Send PDF report": "PDF 보고서 전송",
        "Sending...": "전송 중...",
        "Enter a recipient email address.": "수신자 이메일 주소를 입력하세요.",
        "Athena cost": "Athena 비용",
        "Data scanned": "스캔된 데이터",
        "Chart type": "차트 유형",
        "Could not answer:": "답변 불가：",
        "Request timed out. The query may be complex — try again.": "요청 시간이 초과되었습니다. 다시 시도하세요.",
        "Agent error:": "에이전트 오류：",
        "Could not reach backend:": "백엔드에 연결할 수 없습니다：",
        "EDP Analytics Report": "EDP 분석 보고서",
        "Summary": "요약",
        "SQL Query": "SQL 쿼리",
        "Query Metadata": "쿼리 메타데이터",
        "Query Intent Check": "쿼리 의도 확인",
        "Athena cost:": "Athena 비용：",
        "Data scanned:": "스캔된 데이터：",
        "Chart type:": "차트 유형：",
        "Session": "세션",
        "Started at": "시작 시간",
        "questions_answered_fmt": "{n} 개의 질문 답변됨",
        "Start new session": "새 세션 시작",
        "This will clear all questions. Are you sure?": "모든 질문이 삭제됩니다. 확실합니까?",
        "Yes, clear": "예, 삭제",
        "Cancel": "취소",
        "Export conversation (JSON)": "대화 내보내기 (JSON)",
        "Import conversation (JSON)": "대화 가져오기 (JSON)",
        "History": "기록",
        "restored_fmt": "{n} 개의 대화를 복원했습니다.",
        "Import failed:": "가져오기 실패：",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "어떤 언어로든 Gold 데이터에 대해 질문하세요. 후속 질문은 이전 컨텍스트를 기억합니다.",
        "Try asking:": "질문해 보세요：",
        "rephrase_tips": (
            "**질문 재표현 팁：**\n"
            "- 기간을 구체적으로：*'2024년'*, *'최근 6개월'*\n"
            "- 한 번에 하나의 지표：매출, 주문 수, 배송 기간\n"
            "- 집계 기준 사용：*'국가별'*, *'운송업체별'*, *'제품별'*\n"
            "- Gold 테이블은 사전 집계된 데이터이므로 열린 질문은 피하세요"
        ),
    },
    "ar": {
        "Question": "السؤال",
        "Chart": "الرسم البياني",
        "Table": "الجدول",
        "Details": "التفاصيل",
        "Assumptions": "الافتراضات",
        "Query intent check": "فحص نية الاستعلام",
        "Claude was shown only the SQL (not your question) and asked what it thinks the query is trying to answer:": "تم عرض SQL فقط على Claude (ليس سؤالك) وطُلب منه استنتاج ما يحاول الاستعلام الإجابة عليه:",
        "Inferred:": "المستنتج:",
        "Data quality notice:": "ملاحظة جودة البيانات:",
        "Tips for rephrasing your question": "نصائح لإعادة صياغة سؤالك",
        "Download PDF": "تحميل PDF",
        "Send as email": "إرسال عبر البريد",
        "Close email": "إغلاق البريد",
        "Recipient email address": "عنوان البريد الإلكتروني للمستلم",
        "Send PDF report": "إرسال تقرير PDF",
        "Sending...": "جارٍ الإرسال...",
        "Enter a recipient email address.": "أدخل عنوان البريد الإلكتروني للمستلم.",
        "Athena cost": "تكلفة Athena",
        "Data scanned": "البيانات الممسوحة",
        "Chart type": "نوع الرسم البياني",
        "Could not answer:": "تعذّر الإجابة:",
        "Request timed out. The query may be complex — try again.": "انتهت مهلة الطلب. حاول مرة أخرى.",
        "Agent error:": "خطأ في الوكيل:",
        "Could not reach backend:": "تعذّر الوصول إلى الخادم:",
        "EDP Analytics Report": "تقرير تحليلات EDP",
        "Summary": "الملخص",
        "SQL Query": "استعلام SQL",
        "Query Metadata": "بيانات الاستعلام الوصفية",
        "Query Intent Check": "فحص نية الاستعلام",
        "Athena cost:": "تكلفة Athena:",
        "Data scanned:": "البيانات الممسوحة:",
        "Chart type:": "نوع الرسم البياني:",
        "Session": "الجلسة",
        "Started at": "بدأت في",
        "questions_answered_fmt": "تمت الإجابة على {n} سؤال",
        "Start new session": "بدء جلسة جديدة",
        "This will clear all questions. Are you sure?": "سيؤدي هذا إلى مسح جميع الأسئلة. هل أنت متأكد؟",
        "Yes, clear": "نعم، امسح",
        "Cancel": "إلغاء",
        "Export conversation (JSON)": "تصدير المحادثة (JSON)",
        "Import conversation (JSON)": "استيراد المحادثة (JSON)",
        "History": "السجل",
        "restored_fmt": "تمت استعادة {n} محادثة.",
        "Import failed:": "فشل الاستيراد:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "اطرح أسئلة حول بيانات Gold بأي لغة. الأسئلة المتابعة تتذكر السياق السابق.",
        "Try asking:": "جرّب السؤال:",
        "rephrase_tips": (
            "**نصائح لإعادة الصياغة:**\n"
            "- كن محددًا بالفترات الزمنية: *'عام 2024'*، *'آخر 6 أشهر'*\n"
            "- اسأل عن مقياس واحد في كل مرة: الإيرادات، الطلبات، مدة التسليم\n"
            "- استخدم أبعاد التجميع: *'حسب الدولة'*، *'حسب الناقل'*، *'حسب المنتج'*\n"
            "- تجنب الأسئلة المفتوحة — جداول Gold هي ملخصات مجمعة مسبقًا"
        ),
    },
    "ru": {
        "Question": "Вопрос",
        "Chart": "График",
        "Table": "Таблица",
        "Details": "Подробности",
        "Assumptions": "Допущения",
        "Query intent check": "Проверка намерения запроса",
        "Claude was shown only the SQL (not your question) and asked what it thinks the query is trying to answer:": "Claude видел только SQL (без вашего вопроса) и ответил, что, по его мнению, запрос пытается ответить:",
        "Inferred:": "Выведено:",
        "Data quality notice:": "Уведомление о качестве данных:",
        "Tips for rephrasing your question": "Советы по перефразированию вопроса",
        "Download PDF": "Скачать PDF",
        "Send as email": "Отправить по почте",
        "Close email": "Закрыть письмо",
        "Recipient email address": "Email получателя",
        "Send PDF report": "Отправить PDF отчёт",
        "Sending...": "Отправка...",
        "Enter a recipient email address.": "Введите email получателя.",
        "Athena cost": "Стоимость Athena",
        "Data scanned": "Данные сканированы",
        "Chart type": "Тип графика",
        "Could not answer:": "Не удалось ответить:",
        "Request timed out. The query may be complex — try again.": "Время ожидания истекло. Попробуйте снова.",
        "Agent error:": "Ошибка агента:",
        "Could not reach backend:": "Не удалось подключиться к бэкенду:",
        "EDP Analytics Report": "Аналитический отчёт EDP",
        "Summary": "Резюме",
        "SQL Query": "SQL запрос",
        "Query Metadata": "Метаданные запроса",
        "Query Intent Check": "Проверка намерения запроса",
        "Athena cost:": "Стоимость Athena:",
        "Data scanned:": "Данные сканированы:",
        "Chart type:": "Тип графика:",
        "Session": "Сессия",
        "Started at": "Начало в",
        "questions_answered_fmt": "Отвечено вопросов: {n}",
        "Start new session": "Начать новую сессию",
        "This will clear all questions. Are you sure?": "Все вопросы будут удалены. Вы уверены?",
        "Yes, clear": "Да, очистить",
        "Cancel": "Отмена",
        "Export conversation (JSON)": "Экспорт разговора (JSON)",
        "Import conversation (JSON)": "Импорт разговора (JSON)",
        "History": "История",
        "restored_fmt": "Восстановлено {n} обменов.",
        "Import failed:": "Ошибка импорта:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Задавайте вопросы о данных Gold на любом языке. Дополнительные вопросы помнят предыдущий контекст.",
        "Try asking:": "Попробуйте спросить:",
        "rephrase_tips": (
            "**Советы по перефразированию:**\n"
            "- Уточните временной период: *'в 2024 году'*, *'за последние 6 месяцев'*\n"
            "- Спрашивайте об одной метрике: выручка, заказы, срок доставки\n"
            "- Используйте измерения: *'по стране'*, *'по перевозчику'*, *'по продукту'*\n"
            "- Избегайте открытых вопросов — таблицы Gold содержат агрегированные данные"
        ),
    },
    "el": {
        "Question": "Ερώτηση",
        "Chart": "Γράφημα",
        "Table": "Πίνακας",
        "Details": "Λεπτομέρειες",
        "Assumptions": "Παραδοχές",
        "Query intent check": "Έλεγχος πρόθεσης ερωτήματος",
        "Inferred:": "Συναγόμενο:",
        "Download PDF": "Λήψη PDF",
        "Send as email": "Αποστολή με email",
        "Close email": "Κλείσιμο email",
        "Recipient email address": "Διεύθυνση email παραλήπτη",
        "Send PDF report": "Αποστολή αναφοράς PDF",
        "Sending...": "Αποστολή...",
        "Athena cost": "Κόστος Athena",
        "Data scanned": "Δεδομένα που σαρώθηκαν",
        "Chart type": "Τύπος γραφήματος",
        "EDP Analytics Report": "Αναφορά Analytics EDP",
        "Summary": "Περίληψη",
        "SQL Query": "Ερώτημα SQL",
        "Query Metadata": "Μεταδεδομένα ερωτήματος",
        "Session": "Συνεδρία",
        "Start new session": "Έναρξη νέας συνεδρίας",
        "Cancel": "Ακύρωση",
        "History": "Ιστορικό",
        "questions_answered_fmt": "{n} ερωτήσεις απαντήθηκαν",
        "restored_fmt": "Επαναφέρθηκαν {n} ανταλλαγές.",
        "Try asking:": "Δοκιμάστε να ρωτήσετε:",
    },
    "he": {
        "Question": "שאלה",
        "Chart": "גרף",
        "Table": "טבלה",
        "Details": "פרטים",
        "Assumptions": "הנחות",
        "Query intent check": "בדיקת כוונת השאילתה",
        "Inferred:": "מסקנה:",
        "Download PDF": "הורד PDF",
        "Send as email": 'שלח בדוא"ל',
        "Close email": 'סגור דוא"ל',
        "Recipient email address": 'כתובת דוא"ל הנמען',
        "Send PDF report": "שלח דוח PDF",
        "Sending...": "שולח...",
        "Athena cost": "עלות Athena",
        "Data scanned": "נתונים שנסרקו",
        "Chart type": "סוג גרף",
        "EDP Analytics Report": "דוח Analytics EDP",
        "Summary": "סיכום",
        "SQL Query": "שאילתת SQL",
        "Query Metadata": "מטא-נתוני שאילתה",
        "Session": "סשן",
        "Start new session": "התחל סשן חדש",
        "Cancel": "ביטול",
        "History": "היסטוריה",
        "questions_answered_fmt": "{n} שאלות נענו",
        "restored_fmt": "שוחזרו {n} שיחות.",
        "Try asking:": "נסה לשאול:",
    },
    "th": {
        "Question": "คำถาม",
        "Chart": "แผนภูมิ",
        "Table": "ตาราง",
        "Details": "รายละเอียด",
        "Assumptions": "สมมติฐาน",
        "Query intent check": "ตรวจสอบความตั้งใจของคำค้นหา",
        "Inferred:": "อนุมาน:",
        "Download PDF": "ดาวน์โหลด PDF",
        "Send as email": "ส่งทางอีเมล",
        "Close email": "ปิดอีเมล",
        "Recipient email address": "ที่อยู่อีเมลผู้รับ",
        "Send PDF report": "ส่งรายงาน PDF",
        "Sending...": "กำลังส่ง...",
        "Athena cost": "ต้นทุน Athena",
        "Data scanned": "ข้อมูลที่สแกน",
        "Chart type": "ประเภทแผนภูมิ",
        "EDP Analytics Report": "รายงาน Analytics EDP",
        "Summary": "สรุป",
        "SQL Query": "SQL Query",
        "Query Metadata": "เมทาดาทาของคำค้นหา",
        "Session": "เซสชัน",
        "Start new session": "เริ่มเซสชันใหม่",
        "Cancel": "ยกเลิก",
        "History": "ประวัติ",
        "questions_answered_fmt": "ตอบ {n} คำถามแล้ว",
        "restored_fmt": "กู้คืน {n} การสนทนา",
        "Try asking:": "ลองถาม:",
    },
}


def _t(key: str, lang: str) -> str:
    """Return the translation of a UI string. Falls back to the key (English) if not found."""
    if lang == "en":
        return key
    return _UI_TRANSLATIONS.get(lang, {}).get(key, key)


def _session_language() -> str:
    """Return the language code for the most recent question in history, or 'en'."""
    history = st.session_state.get("history", [])
    if history:
        return _detect_language(history[-1]["question"])
    return "en"


def _t_questions_answered(n: int, lang: str) -> str:
    """Return the '3 questions answered' string in the correct language."""
    if lang == "en":
        return f"{n} question{'s' if n != 1 else ''} answered"
    fmt = _UI_TRANSLATIONS.get(lang, {}).get("questions_answered_fmt", "{n} questions answered")
    return fmt.format(n=n)


def _t_restored(n: int, lang: str) -> str:
    """Return the 'Restored N turns.' string in the correct language."""
    if lang == "en":
        return f"Restored {n} turns."
    fmt = _UI_TRANSLATIONS.get(lang, {}).get("restored_fmt", "Restored {n} turns.")
    return fmt.format(n=n)


# ── Page config ───────────────────────────────────────────────────────────────
# #7: layout="centered" — no CSS max-width hack needed.
st.set_page_config(
    page_title="EDP Analytics Agent",
    page_icon="📊",
    layout="centered",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* ── Reset Streamlit chrome ─────────────────────────────────────────────── */
header[data-testid="stHeader"]  { display: none !important; }
[data-testid="stToolbar"]        { display: none !important; }
[data-testid="stDecoration"]     { display: none !important; }
#MainMenu                        { display: none !important; }
footer                           { display: none !important; }

/* ── Global typography ──────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: "Inter", "Segoe UI", system-ui, sans-serif;
}

/*
 * EDP brand palette
 *   Primary (structural): #4B5320  — army/olive green  (header, sidebar, PDF header)
 *   Accent  (interactive): #6B7A30  — lighter olive     (card stripe, tabs, buttons)
 *   Light tint:            #F3F4EC  — warm off-white    (insight bg, card label bg)
 */

/* ── Branded page header ────────────────────────────────────────────────── */
.edp-header {
    background: linear-gradient(135deg, #3A4118 0%, #4B5320 100%);
    border-radius: 10px;
    padding: 20px 28px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 18px;
}
.edp-header-logo {
    width: 44px;
    height: 44px;
    background: rgba(255,255,255,0.12);
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 22px;
    flex-shrink: 0;
}
.edp-header-text h1 {
    color: #ffffff !important;
    font-size: 22px !important;
    font-weight: 700 !important;
    margin: 0 0 2px 0 !important;
    letter-spacing: -0.3px;
}
.edp-header-text p {
    color: rgba(255,255,255,0.7) !important;
    font-size: 13px !important;
    margin: 0 !important;
}
.edp-header-badge {
    margin-left: auto;
    background: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.2);
    color: #ffffff !important;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    padding: 4px 10px;
    border-radius: 20px;
    white-space: nowrap;
}

/* ── Sidebar ─────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #4B5320 !important;
    border-right: 1px solid #3A4118 !important;
}
/* Force ALL text inside the sidebar to white — catches inline styles too */
[data-testid="stSidebar"] *,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] * {
    color: #ffffff !important;
}
[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.15) !important;
    border: 1px solid rgba(255,255,255,0.3) !important;
    color: #ffffff !important;
    border-radius: 6px !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    transition: background 0.15s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,0.25) !important;
    border-color: rgba(255,255,255,0.5) !important;
}
[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.2) !important;
}
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(255,255,255,0.25) !important;
}
[data-testid="stSidebar"] [data-testid="stDownloadButton"] > button {
    background: rgba(255,255,255,0.15) !important;
    border: 1px solid rgba(255,255,255,0.3) !important;
    color: #ffffff !important;
    font-weight: 600 !important;
}

/* ── Q&A cards (bordered containers) ────────────────────────────────────── */
[data-testid="stVerticalBlockBorderWrapper"] {
    border: none !important;
    border-left: 4px solid #4B5320 !important;
    border-radius: 0 8px 8px 0 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07), 0 0 0 1px rgba(0,0,0,0.04) !important;
    background: #ffffff !important;
    padding: 0 !important;
    margin-bottom: 14px !important;
}

/* ── Card header ─────────────────────────────────────────────────────────── */
.turn-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 6px;
}
.turn-label {
    font-size: 10px;
    font-weight: 700;
    color: #4B5320;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    background: #F3F4EC;
    padding: 2px 8px;
    border-radius: 3px;
}
.turn-time {
    font-size: 11px;
    color: #94a3b8;
    font-variant-numeric: tabular-nums;
}
.turn-question {
    font-size: 17px;
    font-weight: 600;
    color: #0f172a;
    margin: 8px 0 0 0;
    line-height: 1.4;
}

/* ── Insight card ────────────────────────────────────────────────────────── */
.insight-card {
    background: #F3F4EC;
    border-left: 4px solid #4B5320;
    padding: 14px 18px;
    border-radius: 0 6px 6px 0;
    font-size: 15px;
    line-height: 1.75;
    margin: 4px 0 14px 0;
    color: #1e293b;
}

/* ── Status badge ────────────────────────────────────────────────────────── */
.status-badge {
    background: #F3F4EC;
    border: 1px solid #c3c9a0;
    color: #4B5320;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 13px;
    margin: 6px 0;
}

/* ── Tabs ────────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    border-bottom: 2px solid #e2e8f0 !important;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #64748b !important;
    padding: 8px 16px !important;
    border-bottom: 2px solid transparent !important;
    transition: color 0.15s !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: #4B5320 !important;
    border-bottom: 2px solid #4B5320 !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }

/* ── Metrics ─────────────────────────────────────────────────────────────── */
[data-testid="stMetricValue"] {
    font-size: 22px !important;
    font-weight: 700 !important;
    color: #0f172a !important;
}
[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    font-weight: 600 !important;
    color: #64748b !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
}
[data-testid="metric-container"] {
    background: #f8fafc !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    padding: 12px 16px !important;
}

/* ── Expander ────────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    background: #fafafa !important;
}
[data-testid="stExpander"] summary {
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #475569 !important;
}

/* ── Code blocks inside expander ─────────────────────────────────────────── */
[data-testid="stExpander"] [data-testid="stCode"] {
    border-radius: 6px !important;
}

/* ── Download / primary buttons ──────────────────────────────────────────── */
[data-testid="stDownloadButton"] > button,
.stButton > button[kind="primary"] {
    background: #4B5320 !important;
    border: none !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    border-radius: 6px !important;
}
[data-testid="stDownloadButton"] > button:hover,
.stButton > button[kind="primary"]:hover {
    background: #3A4118 !important;
}

/* ── Example question buttons ────────────────────────────────────────────── */
[data-testid="stMainBlockContainer"] .stButton > button {
    border: 1px solid #cbd5e1 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    color: #334155 !important;
    background: #ffffff !important;
    text-align: left !important;
    transition: border-color 0.15s, background 0.15s !important;
}
[data-testid="stMainBlockContainer"] .stButton > button:hover {
    border-color: #4B5320 !important;
    background: #F3F4EC !important;
    color: #4B5320 !important;
}

/* ── Chat input ──────────────────────────────────────────────────────────── */
[data-testid="stChatInput"] {
    border: 2px solid #cbd5e1 !important;
    border-radius: 10px !important;
    transition: border-color 0.15s !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: #4B5320 !important;
    box-shadow: 0 0 0 3px rgba(75,83,32,0.12) !important;
}

/* ── Divider ─────────────────────────────────────────────────────────────── */
hr { border-color: #e2e8f0 !important; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "history" not in st.session_state:
    st.session_state.history = []
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None
if "confirm_clear" not in st.session_state:
    st.session_state.confirm_clear = False
# #12: Track when this session started for display in sidebar.
if "session_start" not in st.session_state:
    st.session_state.session_start = datetime.now(_BERLIN).strftime("%H:%M")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024**2:.2f} MB"


def _format_cost(usd: float) -> str:
    if usd == 0:
        return "—"
    if usd < 0.001:
        return f"${usd:.6f}"
    return f"${usd:.4f}"


def _clean_insight_stream(text: str) -> str:
    """Strip XML tags from partial streaming text for live display."""
    text = re.sub(r"<chart_title>.*?</chart_title>", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]*$", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------


def _extract_kpi_tiles(columns: list[str], rows: list[dict]) -> list[tuple[str, str, str]]:
    """Return up to 3 (metric_label, value, sub_label) KPI tuples from query results."""
    if not rows or not columns:
        return []

    numeric_cols: list[str] = []
    cat_cols: list[str] = []
    for col in columns:
        raw = str(rows[0].get(col, "") or "")
        try:
            float(raw.replace(",", "").replace("$", ""))
            numeric_cols.append(col)
        except ValueError:
            cat_cols.append(col)

    def _fmt(val: str) -> str:
        try:
            f = float(str(val).replace(",", "").replace("$", ""))
            if f == int(f):
                return f"{int(f):,}"
            return f"{f:,.2f}"
        except (ValueError, TypeError):
            return str(val)

    tiles: list[tuple[str, str, str]] = []

    if cat_cols and numeric_cols:
        # Use the LAST numeric column as the primary metric — queries order with
        # the derived/computed metric last (e.g. revenue_per_unit after total_revenue).
        metric_col = numeric_cols[-1]
        cat_col = cat_cols[0]
        metric_label = metric_col.replace("_", " ").title()
        top_cat = str(rows[0].get(cat_col, ""))
        top_val = _fmt(str(rows[0].get(metric_col, "")))
        tiles.append((metric_label, top_val, top_cat))
        # Proper plural for the category sub-label (category → Categories, country → Countries)
        cat_label = cat_col.replace("_", " ").title()
        if cat_label.endswith("y"):
            cat_plural = cat_label[:-1] + "ies"
        else:
            cat_plural = cat_label + "s"
        tiles.append(("Total Entries", str(len(rows)), cat_plural))
        # Third tile: sum of the primary metric across all rows (meaningful aggregate)
        try:
            total = sum(float(str(r.get(metric_col, 0) or 0).replace(",", "")) for r in rows)
            # Avoid "Total Total Revenue" — strip leading "Total" from the metric label.
            base_label = re.sub(r"^Total\s+", "", metric_label, flags=re.IGNORECASE)
            tiles.append((f"Total {base_label} (All)", _fmt(str(total)), "All Entries"))
        except (ValueError, TypeError):
            pass
    elif numeric_cols:
        # No categorical column — show top 3 numeric values from row 0
        for col in numeric_cols[:3]:
            val = _fmt(str(rows[0].get(col, "")))
            label = col.replace("_", " ").title()
            tiles.append((label, val, ""))
    else:
        tiles.append(("Total Results", str(len(rows)), ""))

    return tiles[:3]


def _plain_english_assumption(text: str) -> str:
    """Strip SQL and markdown artefacts from an assumption string for stakeholder PDFs.

    Applied in order:
      1. Strip markdown bold/italic markers (**text** → text, *text* → text)
      2. Strip backtick wrappers (`name` → name)
      3. Remove fully-qualified DB prefixes (edp_dev_gold.table → table)
      4. Remove dbt internal table name prefixes: int_, stg_, fct_, dim_, rpt_
      5. Replace remaining snake_case identifiers (word_word) with Title Case words
      6. Normalise whitespace
    """
    # 1. Strip markdown bold/italic (up to triple asterisk)
    text = re.sub(r"\*{1,3}([^*\n]+)\*{1,3}", r"\1", text)
    # 2. Strip backtick wrappers
    text = re.sub(r"`([^`]+)`", r"\1", text)
    # 3. Remove database prefix patterns (e.g. edp_dev_gold.)
    text = re.sub(r"edp_\w+\.", "", text)
    # 4. Remove dbt layer prefixes on table names (int_, stg_, fct_, dim_, rpt_)
    text = re.sub(
        r"\b(int|stg|fct|dim|rpt)_([a-z][a-z0-9_]*)", lambda m: m.group(2).replace("_", " "), text
    )
    # 5. Replace remaining snake_case column/table identifiers with readable words
    #    Only replace sequences that look like column names (2+ words joined by underscore)
    text = re.sub(
        r"\b([a-z][a-z0-9]+)_([a-z][a-z0-9_]+)\b",
        lambda m: (m.group(1) + " " + m.group(2).replace("_", " ")),
        text,
    )
    # 6. Normalise whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _draw_e_mark(pdf: Any, x: float, y: float, size: float = 10.0) -> None:
    """Draw the Geometric E logo mark (navy square, white E) at position (x, y).

    The E is faithful to Concept 3 from the brand SVG: three horizontal bars
    (top, shorter middle, bottom) plus a left vertical bar, all white on navy.

    Args:
        pdf: fpdf2 FPDF instance.
        x, y: Top-left corner of the mark in mm.
        size: Side length of the square in mm.
    """
    # Deep navy square background
    pdf.set_fill_color(75, 83, 32)  # #4B5320 army olive
    pdf.rect(x, y, size, size, style="F")

    # White E bars — proportions derived from the brand SVG (80x80 unit grid)
    pad = size * 0.175  # left/top padding
    bar_h = size * 0.075  # bar height
    bar_w_full = size * 0.525  # top/bottom bar width
    bar_w_mid = size * 0.375  # middle bar width (shorter)
    vert_w = size * 0.075  # vertical bar width
    e_height = size * 0.55  # full height of E

    pdf.set_fill_color(255, 255, 255)
    # Vertical bar (spine of the E)
    pdf.rect(x + pad, y + pad + (size - 2 * pad - e_height) / 2, vert_w, e_height, style="F")
    # Top bar
    pdf.rect(
        x + pad,
        y + pad + (size - 2 * pad - e_height) / 2,
        bar_w_full,
        bar_h,
        style="F",
    )
    # Middle bar (centred vertically within the E)
    mid_y = y + pad + (size - 2 * pad - e_height) / 2 + (e_height - bar_h) / 2
    pdf.rect(x + pad, mid_y, bar_w_mid, bar_h, style="F")
    # Bottom bar
    pdf.rect(
        x + pad,
        y + pad + (size - 2 * pad - e_height) / 2 + e_height - bar_h,
        bar_w_full,
        bar_h,
        style="F",
    )
    # Reset fill
    pdf.set_fill_color(255, 255, 255)


# #3: PDF generation is cached. Individual hashable fields are the cache key so
# Streamlit avoids rebuilding PDFs on every rerun for historical turns.
@st.cache_data(show_spinner=False)
def _cached_build_pdf(
    question: str,
    insight: str,
    assumptions_json: str,
    png_b64: str,
    columns_json: str,
    rows_json: str,
) -> bytes:
    """Build an enterprise-grade two-page PDF stakeholder report.

    Page 1: Header (logo + name + classification) | Question | KPI tiles |
            Chart | Key Finding | Footer.
    Page 2: Header | Methodology (plain-English assumptions) | Footer.
    """
    import datetime
    import io
    import zoneinfo as _zi

    from fpdf import FPDF

    lang = _detect_language(question)
    assumptions: list[str] = json.loads(assumptions_json) if assumptions_json else []
    columns: list[str] = json.loads(columns_json) if columns_json else []
    rows: list[dict] = json.loads(rows_json) if rows_json else []

    generated_str = datetime.datetime.now(_zi.ZoneInfo("Europe/Berlin")).strftime(
        "%d %B %Y, %H:%M %Z"
    )

    # ── Font selection ──────────────────────────────────────────────────────
    _NOTO_CJK_PATHS = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
    ] + _glob.glob("/usr/share/fonts/**/*[Nn]oto*[Cc][Jj][Kk]*", recursive=True)
    _DEJAVU_DIR = "/usr/share/fonts/truetype/dejavu"

    # Resolve font name before instantiating _PDFReport (header() needs it).
    _font_name_resolved = "Helvetica"
    _font_map: dict[str, str] = {}

    # We'll register fonts after instantiation; store paths for later.
    _cjk_path_to_use: str | None = None
    if lang in ("zh", "ja", "ko"):
        for cjk_path in _NOTO_CJK_PATHS:
            if os.path.exists(cjk_path):
                _cjk_path_to_use = cjk_path
                _font_name_resolved = "NotoSansCJK"
                break
    if _font_name_resolved == "Helvetica":
        dejavu_reg = f"{_DEJAVU_DIR}/DejaVuSans.ttf"
        dejavu_bold = f"{_DEJAVU_DIR}/DejaVuSans-Bold.ttf"
        if os.path.exists(dejavu_reg):
            _font_name_resolved = "DejaVu"
            _font_map["reg"] = dejavu_reg
            _font_map["bold"] = dejavu_bold if os.path.exists(dejavu_bold) else dejavu_reg

    font_name = _font_name_resolved

    # ── FPDF subclass with enterprise header + footer on every page ─────────
    # Captures font_name, lang, generated_str from the enclosing scope.
    class _PDFReport(FPDF):
        _fn: str = font_name
        _lang: str = lang
        _gen: str = generated_str

        def header(self) -> None:
            # Full-width navy header strip (25 mm tall, edge-to-edge)
            self.set_fill_color(75, 83, 32)  # #4B5320 army olive
            self.rect(0, 0, self.w, 25, style="F")

            # Geometric E mark — 12 mm square, vertically centred in strip
            _draw_e_mark(self, x=7, y=6.5, size=12)

            # Name + title (left of mark)
            self.set_xy(22, 7)
            self.set_font(self._fn, "B", 9)
            self.set_text_color(255, 255, 255)
            self.cell(55, 5, "EMEKA EDEH")
            self.set_xy(22, 13)
            self.set_font(self._fn, "", 7)
            self.set_text_color(180, 210, 230)
            self.cell(55, 4, "DATA ENGINEER")

            # Centre: report title
            report_title = _t("EDP Analytics Report", self._lang)
            self.set_font(self._fn, "B", 10)
            self.set_text_color(255, 255, 255)
            title_w = 70.0
            self.set_xy(self.w / 2 - title_w / 2, 9)
            self.cell(title_w, 6, report_title, align="C")

            # Right: classification + generation timestamp
            self.set_font(self._fn, "", 7)
            self.set_text_color(180, 210, 230)
            self.set_xy(self.w - 62, 7)
            self.cell(57, 5, "INTERNAL  |  CONFIDENTIAL", align="R")
            self.set_xy(self.w - 62, 13)
            self.set_font(self._fn, "", 6)
            self.cell(57, 4, f"Generated: {self._gen}", align="R")

            # Reset colour; content starts at y=28
            self.set_text_color(0, 0, 0)
            self.set_y(28)

        def footer(self) -> None:
            self.set_y(-13)
            col_w = self.epw / 3
            self.set_font("Helvetica", "", 7)
            self.set_text_color(148, 163, 184)
            self.set_x(self.l_margin)
            self.cell(col_w, 5, f"Data as of {self._gen}", align="L")
            self.cell(col_w, 5, "Confidential - Internal Use Only", align="C")
            self.cell(col_w, 5, f"Page {self.page_no()} of {{nb}}", align="R")
            self.set_text_color(0, 0, 0)

    pdf = _PDFReport()
    pdf.alias_nb_pages()
    # Top margin 28 mm matches the header strip height. Left/right 15 mm.
    pdf.set_margins(15, 28, 15)
    pdf.set_auto_page_break(auto=True, margin=18)

    # Register fonts now (after instantiation, before add_page).
    if _cjk_path_to_use:
        try:
            pdf.add_font("NotoSansCJK", fname=_cjk_path_to_use)
            try:
                pdf.add_font("NotoSansCJK", style="B", fname=_cjk_path_to_use)
            except Exception:
                pass
        except Exception:
            font_name = "Helvetica"
            pdf._fn = "Helvetica"
    elif font_name == "DejaVu":
        try:
            pdf.add_font("DejaVu", fname=_font_map["reg"])
            pdf.add_font("DejaVu", style="B", fname=_font_map["bold"])
        except Exception:
            font_name = "Helvetica"
            pdf._fn = "Helvetica"

    # Sync resolved font name onto the subclass attribute (header uses it).
    pdf._fn = font_name  # type: ignore[attr-defined]

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 1 — Question · KPI tiles · Chart · Key Finding
    # ════════════════════════════════════════════════════════════════════════
    pdf.add_page()
    W = pdf.epw  # effective page width inside margins

    # ── Question ─────────────────────────────────────────────────────────────
    pdf.set_font(font_name, "B", 11)
    pdf.set_text_color(75, 83, 32)  # #4B5320 army olive label
    pdf.cell(W, 5, _t("Question", lang).upper(), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)  # slate-200 rule
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
    pdf.ln(3)
    pdf.set_font(font_name, "B", 13)
    pdf.set_text_color(15, 23, 42)  # slate-950
    pdf.multi_cell(W, 7, question, align="L")
    pdf.ln(8)

    # ── KPI tiles ─────────────────────────────────────────────────────────────
    kpi_tiles = _extract_kpi_tiles(columns, rows)
    if kpi_tiles:
        tile_w = W / len(kpi_tiles)
        tile_h = 22.0
        tile_y = pdf.get_y()
        for i, (metric_lbl, value, sub_lbl) in enumerate(kpi_tiles):
            tx = pdf.l_margin + i * tile_w
            # Tile background
            pdf.set_fill_color(240, 247, 255)  # #f0f7ff blue-50
            pdf.rect(tx, tile_y, tile_w - 2, tile_h, style="F")
            # Top navy accent stripe (3 mm) — deep navy primary
            pdf.set_fill_color(75, 83, 32)  # #4B5320 army olive
            pdf.rect(tx, tile_y, tile_w - 2, 3, style="F")
            # Metric label (small caps)
            pdf.set_xy(tx + 3, tile_y + 4)
            pdf.set_font(font_name, "", 7)
            pdf.set_text_color(75, 83, 32)  # #4B5320 army olive
            pdf.cell(tile_w - 5, 4, metric_lbl.upper(), align="L")
            # Value (large bold)
            pdf.set_xy(tx + 3, tile_y + 9)
            pdf.set_font(font_name, "B", 14)
            pdf.set_text_color(15, 23, 42)
            pdf.cell(tile_w - 5, 7, value, align="L")
            # Sub-label
            pdf.set_xy(tx + 3, tile_y + 16)
            pdf.set_font(font_name, "", 7)
            pdf.set_text_color(75, 83, 32)  # #4B5320 army olive
            pdf.cell(tile_w - 5, 4, sub_lbl, align="L")

        pdf.set_y(tile_y + tile_h + 6)
        pdf.set_text_color(0, 0, 0)
        pdf.set_fill_color(255, 255, 255)

    # ── Chart image ──────────────────────────────────────────────────────────
    if png_b64:
        png_bytes = _b64.b64decode(png_b64)
        pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin, w=W)
        pdf.ln(6)

    # ── Key Finding (insight) — blue left-accent bar ──────────────────────────
    pdf_insight = re.sub(r"\*{1,3}([^*\n]+)\*{1,3}", r"\1", insight)
    pdf_insight = pdf_insight.replace("\r\n", "\n").replace("\r", "\n")
    pdf.set_font(font_name, "B", 11)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(W, 5, _t("Summary", lang).upper(), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
    pdf.ln(4)
    pdf.set_font(font_name, "", 11)
    pdf.set_text_color(30, 41, 59)  # slate-800
    y_before = pdf.get_y()
    pdf.set_x(pdf.l_margin + 6)
    pdf.multi_cell(W - 6, 7, pdf_insight, align="L")
    y_after = pdf.get_y()
    pdf.set_fill_color(75, 83, 32)  # #4B5320 army olive
    pdf.rect(pdf.l_margin, y_before, 3, y_after - y_before, style="F")
    pdf.set_fill_color(255, 255, 255)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 2 — Methodology (plain-English assumptions, no SQL, no tech names)
    # ════════════════════════════════════════════════════════════════════════
    if assumptions:
        pdf.add_page()

        # ── Methodology — plain-English rewrite of assumptions ───────────────
        pdf.set_font(font_name, "B", 11)
        pdf.set_text_color(100, 116, 139)
        pdf.cell(W, 5, "METHODOLOGY", new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(226, 232, 240)
        pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
        pdf.ln(4)
        pdf.set_font(font_name, "", 11)
        pdf.set_text_color(30, 41, 59)
        y_before_a = pdf.get_y()
        for item in assumptions:
            clean = _plain_english_assumption(item)
            pdf.set_x(pdf.l_margin + 6)
            pdf.multi_cell(W - 6, 7, f"\u2022 {clean}", align="L")
            pdf.ln(1)
        y_after_a = pdf.get_y()
        pdf.set_fill_color(75, 83, 32)  # #4B5320 army olive
        pdf.rect(pdf.l_margin, y_before_a, 3, y_after_a - y_before_a, style="F")
        pdf.set_fill_color(255, 255, 255)

    return bytes(pdf.output())


def _build_pdf(turn: dict) -> bytes:
    """Wrapper that extracts hashable fields from a turn dict and calls the cached builder."""
    return _cached_build_pdf(
        question=turn["question"],
        insight=turn["insight"],
        assumptions_json=json.dumps(turn.get("assumptions", [])),
        png_b64=turn.get("png_b64") or "",
        columns_json=json.dumps(turn.get("columns", [])),
        rows_json=json.dumps(turn.get("rows", [])),
    )


def _render_turn(turn: dict, form_key: str) -> None:
    """Render one Q&A turn's answer content. Called inside a turn card."""
    lang = _detect_language(turn["question"])
    is_analytical = bool(turn.get("sql"))

    # Insight card
    escaped = html_lib.escape(turn["insight"]).replace("\n", "<br>")
    st.markdown(f'<div class="insight-card">{escaped}</div>', unsafe_allow_html=True)

    # Validation flags
    for flag in turn.get("validation_flags", []):
        st.warning(f"{_t('Data quality notice:', lang)} {flag}")

    # #8: st.tabs replaces st.radio for chart/table toggle.
    if turn.get("html_chart"):
        chart_h = turn.get("chart_height", 400)
        has_raw = bool(turn.get("columns") and turn.get("rows"))
        if has_raw:
            tab_chart, tab_table = st.tabs([_t("Chart", lang), _t("Table", lang)])
            with tab_chart:
                # #5: +20px buffer so Plotly content never clips silently.
                components.html(turn["html_chart"], height=chart_h + 20, scrolling=False)
            with tab_table:
                df = pd.DataFrame(turn["rows"])
                st.dataframe(df, use_container_width=True)
        else:
            components.html(turn["html_chart"], height=chart_h + 20, scrolling=False)

    # #2: One row of action buttons. Download is a direct button; email toggles
    # an inline form via session state so the user doesn't need to hunt for it.
    # #6: PDF filename includes the turn number so browser downloads don't overwrite.
    pdf_num = int(form_key) + 1 if form_key.isdigit() else len(st.session_state.history) + 1
    try:
        pdf_bytes = _build_pdf(turn)  # #3: served from cache on reruns
        col_dl, col_email, _ = st.columns([1, 1, 2])
        with col_dl:
            st.download_button(
                _t("Download PDF", lang),
                data=pdf_bytes,
                file_name=f"edp_report_q{pdf_num}.pdf",
                mime="application/pdf",
                key=f"pdf_{form_key}",
            )
        with col_email:
            email_open_key = f"email_open_{form_key}"
            label = (
                _t("Close email", lang)
                if st.session_state.get(email_open_key)
                else _t("Send as email", lang)
            )
            if st.button(label, key=f"email_toggle_{form_key}"):
                st.session_state[email_open_key] = not st.session_state.get(email_open_key, False)
                st.rerun()
    except ImportError:
        st.caption("PDF unavailable — run `pip install fpdf2` to enable downloads.")
    except Exception as exc:  # noqa: BLE001
        st.caption(f"PDF generation failed: {exc}")

    # Email inline form (shown when toggle is active)
    if st.session_state.get(f"email_open_{form_key}"):
        with st.form(key=f"email_{form_key}"):
            to_email = st.text_input(_t("Recipient email address", lang))
            send = st.form_submit_button(_t("Send PDF report", lang))
        if send:
            if not to_email:
                st.warning(_t("Enter a recipient email address.", lang))
            else:
                with st.spinner(_t("Sending...", lang)):
                    try:
                        pdf_bytes = _build_pdf(turn)
                        r = requests.post(
                            f"{BACKEND_URL}/send-report",
                            json={
                                "to_email": to_email,
                                "question": turn["question"],
                                "pdf_b64": _b64.b64encode(pdf_bytes).decode(),
                            },
                            timeout=30,
                        )
                        r.raise_for_status()
                        st.success(f"Report sent to {to_email}")
                        st.session_state[f"email_open_{form_key}"] = False
                    except requests.exceptions.HTTPError as exc:
                        detail = (
                            exc.response.json().get("detail", str(exc))
                            if exc.response
                            else str(exc)
                        )
                        st.error(f"Failed: {detail}")
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Failed: {exc}")

    # #2 + #4: Single "Details" expander — SQL, cost metrics, assumptions,
    # and query intent all in one place. Intent is rendered directly (no
    # button/rerun) since inferred_question is already in the turn dict.
    if is_analytical:
        with st.expander(_t("Details", lang)):
            st.code(turn["sql"], language="sql")
            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric(_t("Athena cost", lang), _format_cost(turn["cost_usd"]))
            c2.metric(_t("Data scanned", lang), _format_bytes(turn["bytes_scanned"]))
            c3.metric(_t("Chart type", lang), (turn.get("chart_type") or "—").title())

            if turn.get("assumptions"):
                st.divider()
                st.caption(f"**{_t('Assumptions', lang)}**")
                for item in turn["assumptions"]:
                    st.caption(f"• {item}")

            if turn.get("inferred_question"):
                st.divider()
                st.caption(f"**{_t('Query intent check', lang)}**")
                st.caption(
                    _t(
                        "Claude was shown only the SQL (not your question) and asked "
                        "what it thinks the query is trying to answer:",
                        lang,
                    )
                )
                escaped_inferred = html_lib.escape(turn["inferred_question"])
                inferred_label = _t("Inferred:", lang)
                st.markdown(
                    f'<div style="background:#f8fafc;border-left:3px solid #94a3b8;'
                    f"padding:10px 14px;border-radius:0 4px 4px 0;font-size:14px;"
                    f'color:#334155;margin:4px 0;">'
                    f"<strong>{inferred_label}</strong> {escaped_inferred}</div>",
                    unsafe_allow_html=True,
                )
                verdict = turn.get("verdict", "No")
                discrepancy_detail = turn.get("discrepancy_detail", "None")
                if verdict == "Yes":
                    st.warning(
                        f"Intent mismatch detected: {discrepancy_detail}",
                        icon="⚠️",
                    )
                else:
                    st.success(_t("Intent matches your question.", lang), icon="✅")
    elif turn.get("assumptions"):
        with st.expander(_t("Details", lang)):
            st.caption(f"**{_t('Assumptions', lang)}**")
            for item in turn["assumptions"]:
                st.caption(f"• {item}")


def _render_card(turn: dict, turn_number: int, form_key: str) -> None:
    """Render a complete turn: numbered card with question header and answer content."""
    lang = _detect_language(turn["question"])
    ts = turn.get("timestamp", "")
    # #1: st.container(border=True) replaces st.chat_message. Clean bordered card,
    # no chat bubble, no avatar — looks like a data product not a chatbot.
    # Pre-build the timestamp span to avoid backslash-in-f-string (Python 3.11).
    ts_span = f'<span class="turn-time">{ts}</span>' if ts else ""
    question_label = _t("Question", lang)
    with st.container(border=True):
        st.markdown(
            f'<div class="turn-meta">'
            f'<span class="turn-label">{question_label} {turn_number}</span>'
            f"{ts_span}"
            f"</div>"
            f'<div class="turn-question">{html_lib.escape(turn["question"])}</div>',
            unsafe_allow_html=True,
        )
        st.divider()
        _render_turn(turn, form_key=form_key)


@st.cache_data(ttl=300, show_spinner=False)
def _load_examples() -> list[str]:
    """Fetch example questions from the backend. Falls back to hardcoded list."""
    try:
        r = requests.get(f"{BACKEND_URL}/examples", timeout=5)
        r.raise_for_status()
        questions = r.json().get("questions", [])
        if questions:
            return questions
    except Exception:  # noqa: BLE001
        pass
    return _FALLBACK_EXAMPLES


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    sl = _session_language()
    n = len(st.session_state.history)
    st.markdown(
        f"""
<div style="padding:6px 0 18px 0;">
  <div style="font-size:11px;font-weight:700;color:rgba(255,255,255,0.6);text-transform:uppercase;
              letter-spacing:0.12em;margin-bottom:12px;">Session</div>
  <div style="font-size:13px;color:rgba(255,255,255,0.75);margin-bottom:4px;">
    Started at <strong style="color:#ffffff">{st.session_state.session_start}</strong>
  </div>
  <div style="font-size:13px;color:rgba(255,255,255,0.75);">
    <strong style="color:#ffffff">{n}</strong> question{"s" if n != 1 else ""} answered
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    # #10: Confirm before wiping the session. Accidental click on "Start new
    # session" would previously clear everything with no way to recover.
    st.divider()
    if not st.session_state.confirm_clear:
        if st.button(_t("Start new session", sl), use_container_width=True):
            st.session_state.confirm_clear = True
            st.rerun()
    else:
        st.warning(_t("This will clear all questions. Are you sure?", sl))
        col_yes, col_no = st.columns(2)
        if col_yes.button(_t("Yes, clear", sl), type="primary", use_container_width=True):
            st.session_state.session_id = None
            st.session_state.history = []
            st.session_state.confirm_clear = False
            st.session_state.session_start = datetime.now(_BERLIN).strftime("%H:%M")
            st.rerun()
        if col_no.button(_t("Cancel", sl), use_container_width=True):
            st.session_state.confirm_clear = False
            st.rerun()

    if st.session_state.history:
        st.divider()
        export_data = [
            {
                "question": t["question"],
                "insight": t["insight"],
                "sql": t.get("sql", ""),
                "assumptions": t.get("assumptions", []),
                "inferred_question": t.get("inferred_question", ""),
                "verdict": t.get("verdict", "No"),
                "discrepancy_detail": t.get("discrepancy_detail", "None"),
                "request_id": t.get("request_id", ""),
                "cost_usd": t.get("cost_usd", 0.0),
                "bytes_scanned": t.get("bytes_scanned", 0),
                "timestamp": t.get("timestamp", ""),
            }
            for t in st.session_state.history
        ]
        st.download_button(
            _t("Export conversation (JSON)", sl),
            data=json.dumps(export_data, indent=2, ensure_ascii=False),
            file_name="edp_conversation.json",
            mime="application/json",
            use_container_width=True,
        )

        # Engineer log download (Option A: one CSV file per request on S3).
        # Two-phase: "Prepare" fetches and caches in session_state so the
        # st.download_button is always rendered on the same frame as the data.
        if st.session_state.session_id:
            # Clear cached log when the session changes.
            if st.session_state.get("_log_sid") != st.session_state.session_id:
                st.session_state["_log_csv"] = None
                st.session_state["_log_rows"] = 0
                st.session_state["_log_sid"] = st.session_state.session_id

            if st.session_state.get("_log_csv") is None:
                # Phase 1: fetch button
                if st.button(_t("Prepare Session Log (CSV)", sl), use_container_width=True):
                    try:
                        r = requests.get(
                            f"{BACKEND_URL}/engineer-log",
                            params={"session_id": st.session_state.session_id},
                            timeout=15,
                        )
                        r.raise_for_status()
                        payload = r.json()
                        st.session_state["_log_csv"] = payload.get("csv", "") or ""
                        st.session_state["_log_rows"] = payload.get("row_count", 0)
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Could not fetch log: {exc}")
                    st.rerun()
            else:
                # Phase 2: save button — always visible after fetch
                log_label = (
                    f"{_t('Download Session Log', sl)} ({st.session_state['_log_rows']} rows)"
                )
                if st.session_state["_log_csv"]:
                    st.download_button(
                        label=log_label,
                        data=st.session_state["_log_csv"].encode("utf-8"),
                        file_name=f"edp_engineer_log_{st.session_state.session_id[:8]}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.caption(_t("No log entries yet for this session.", sl))
                if st.button(_t("Refresh log", sl), use_container_width=True):
                    st.session_state["_log_csv"] = None
                    st.rerun()

        st.divider()
        st.caption(f"**{_t('History', sl)}**")
        for i, t in enumerate(st.session_state.history, 1):
            q = t["question"]
            label = q if len(q) <= 42 else q[:39] + "..."
            st.caption(f"{i}. {label}")

    # Import conversation
    st.divider()
    uploaded = st.file_uploader(
        _t("Import conversation (JSON)", sl), type="json", key="import_file"
    )
    if uploaded is not None:
        try:
            imported: list[dict] = json.loads(uploaded.read())
            st.session_state.history = [
                {
                    "question": t["question"],
                    "insight": t["insight"],
                    "sql": t.get("sql", ""),
                    "assumptions": t.get("assumptions", []),
                    "html_chart": None,
                    "png_b64": None,
                    "cost_usd": 0.0,
                    "bytes_scanned": 0,
                    "validation_flags": [],
                    "chart_type": "",
                    "inferred_question": "",
                    "columns": [],
                    "rows": [],
                    "chart_height": 0,
                    "timestamp": "",
                    "request_id": "",
                    "verdict": "No",
                    "discrepancy_detail": "None",
                }
                for t in imported
            ]
            st.session_state.session_id = None
            st.success(_t_restored(len(imported), sl))
            st.rerun()
        except Exception as exc:  # noqa: BLE001
            st.error(f"{_t('Import failed:', sl)} {exc}")

# ── Page header ───────────────────────────────────────────────────────────────
hl = _session_language()
_subtitle = html_lib.escape(
    _t(
        "Ask questions about your Gold data in any language. "
        "Follow-up questions remember prior context.",
        hl,
    )
)
st.markdown(
    f"""
<div class="edp-header">
  <div class="edp-header-logo">&#x1F4CA;</div>
  <div class="edp-header-text">
    <h1>EDP Analytics Agent</h1>
    <p>{_subtitle}</p>
  </div>
  <div class="edp-header-badge">Gold Layer &nbsp;&#x25CF;&nbsp; Live</div>
</div>
""",
    unsafe_allow_html=True,
)

# ── Empty state — example questions ──────────────────────────────────────────
if not st.session_state.history:
    st.markdown(f"#### {_t('Try asking:', 'en')}")
    example_questions = _load_examples()
    col_a, col_b = st.columns(2)
    for i, eq in enumerate(example_questions):
        target_col = col_a if i % 2 == 0 else col_b
        if target_col.button(eq, key=f"ex_{i}", use_container_width=True):
            st.session_state.pending_question = eq
            st.rerun()

# ── Conversation history ──────────────────────────────────────────────────────
# #1: Each turn is a numbered card, not a chat bubble.
for idx, turn in enumerate(st.session_state.history):
    _render_card(turn, turn_number=idx + 1, form_key=str(idx))

# ── Question input ────────────────────────────────────────────────────────────
chat_question = st.chat_input("Ask a question about your data...")
question = chat_question or st.session_state.pending_question
if st.session_state.pending_question:
    st.session_state.pending_question = None

if question:
    lang = _detect_language(question)
    n = len(st.session_state.history) + 1
    now_str = datetime.now(_BERLIN).strftime("%H:%M")
    question_label = _t("Question", lang)

    # #1: Live turn also uses the card container so it matches history cards.
    with st.container(border=True):
        st.markdown(
            f'<div class="turn-meta">'
            f'<span class="turn-label">{question_label} {n}</span>'
            f'<span class="turn-time">{now_str}</span>'
            f"</div>"
            f'<div class="turn-question">{html_lib.escape(question)}</div>',
            unsafe_allow_html=True,
        )
        st.divider()

        _status = st.empty()
        _insight_stream = st.empty()

        payload: dict = {"question": question}
        if st.session_state.session_id:
            payload["session_id"] = st.session_state.session_id

        turn_data: dict | None = None
        streamed_insight = ""

        try:
            with requests.post(
                f"{BACKEND_URL}/ask/stream",
                json=payload,
                stream=True,
                timeout=120,
            ) as resp:
                resp.raise_for_status()
                for raw_line in resp.iter_lines():
                    if not raw_line:
                        continue
                    try:
                        event = json.loads(raw_line)
                    except Exception:  # noqa: BLE001
                        continue

                    etype = event.get("type")
                    if etype == "status":
                        # #11: Styled badge replaces invisible grey caption.
                        _status.markdown(
                            f'<div class="status-badge">⏳ {html_lib.escape(event["text"])}</div>',
                            unsafe_allow_html=True,
                        )
                    elif etype == "token":
                        streamed_insight += event["text"]
                        cleaned = _clean_insight_stream(streamed_insight)
                        if cleaned:
                            _insight_stream.markdown(
                                f'<div class="insight-card">'
                                f"{html_lib.escape(cleaned).replace(chr(10), '<br>')}"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                    elif etype == "error":
                        _status.empty()
                        _insight_stream.empty()
                        st.error(f"{_t('Could not answer:', lang)} {event['text']}")
                        tips = _UI_TRANSLATIONS.get(lang, {}).get("rephrase_tips", _REPHRASE_TIPS)
                        with st.expander(_t("Tips for rephrasing your question", lang)):
                            st.markdown(tips)
                        st.stop()
                    elif etype == "done":
                        turn_data = event["data"]
                        _status.empty()
                        _insight_stream.empty()

        except requests.exceptions.Timeout:
            _status.empty()
            _insight_stream.empty()
            st.error(_t("Request timed out. The query may be complex — try again.", lang))
            st.stop()
        except requests.exceptions.HTTPError as exc:
            _status.empty()
            _insight_stream.empty()
            detail = exc.response.json().get("detail", str(exc)) if exc.response else str(exc)
            st.error(f"{_t('Agent error:', lang)} {detail}")
            st.stop()
        except requests.exceptions.RequestException as exc:
            _status.empty()
            _insight_stream.empty()
            st.error(f"{_t('Could not reach backend:', lang)} {exc}")
            st.stop()

        if turn_data:
            st.session_state.session_id = turn_data["session_id"]

            turn = {
                "question": question,
                "insight": turn_data["insight"],
                "assumptions": turn_data.get("assumptions", []),
                "html_chart": turn_data.get("html_chart"),
                "sql": turn_data.get("sql", ""),
                "png_b64": turn_data.get("png_b64"),
                "cost_usd": turn_data.get("cost_usd", 0.0),
                "bytes_scanned": turn_data.get("bytes_scanned", 0),
                "validation_flags": turn_data.get("validation_flags", []),
                "chart_type": turn_data.get("chart_type", ""),
                "inferred_question": turn_data.get("inferred_question", ""),
                "columns": turn_data.get("columns", []),
                "rows": turn_data.get("rows", []),
                "chart_height": turn_data.get("chart_height", 400),
                "timestamp": now_str,  # #12: stored so card header shows it on replay
                "request_id": turn_data.get("request_id", ""),
                "verdict": turn_data.get("verdict", "No"),
                "discrepancy_detail": turn_data.get("discrepancy_detail", "None"),
            }
            _render_turn(turn, form_key="current")
            st.session_state.history.append(turn)
