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
    """Detect the dominant script/language from Unicode ranges and Latin-script markers.

    Non-Latin scripts (CJK, Arabic, Cyrillic, etc.) are detected by Unicode block.
    For Latin-script languages, a second pass checks language-specific characters
    and high-frequency function words to distinguish German, Italian, French,
    Spanish, and Portuguese from English.

    Returns a language code or 'en' as the fallback.
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
    if counts:
        return max(counts, key=lambda k: counts[k])

    # Latin-script language detection via distinctive characters and common words.
    tl = text.lower()
    words = set(tl.split())

    # German: ä/ö/ü/ß are highly distinctive; also check common German function words.
    if (
        any(c in text for c in "äöüßÄÖÜ")
        or len(
            {
                "der",
                "die",
                "das",
                "und",
                "welche",
                "welcher",
                "nicht",
                "von",
                "mit",
                "haben",
                "ist",
                "sind",
            }
            & words
        )
        >= 2
    ):
        return "de"

    # Italian: accented è plus common Italian words.
    if (
        "è" in text
        or len(
            {
                "il",
                "la",
                "le",
                "per",
                "con",
                "che",
                "del",
                "delle",
                "degli",
                "sono",
                "questo",
                "qual",
            }
            & words
        )
        >= 2
    ):
        return "it"

    # French: ê/â/î/ô/û/ç/œ are fairly French-specific; also check common words.
    if (
        any(c in text for c in "êâîôûçœæÊÂÎÔÛÇ")
        or len(
            {"les", "des", "est", "dans", "avec", "sont", "cette", "pour", "qui", "vous", "nous"}
            & words
        )
        >= 2
    ):
        return "fr"

    # Portuguese: ã/õ are very distinctive (not in Spanish or French).
    if (
        any(c in text for c in "ãõÃÕ")
        or len({"dos", "das", "numa", "pelos", "pelas", "você", "nossa", "pelo"} & words) >= 2
    ):
        return "pt"

    # Spanish: ñ/¿/¡ are distinctive; also check Spanish-specific words.
    if (
        any(c in text for c in "ñÑ¿¡")
        or len(
            {"los", "las", "del", "una", "por", "para", "con", "que", "son", "cuál", "cuáles"}
            & words
        )
        >= 2
    ):
        return "es"

    return "en"


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


# ── Extra PDF/KPI strings + Latin-script language entries ─────────────────────
# Merged into _UI_TRANSLATIONS so all languages share the same _t() lookup.
_EXTRA_TRANSLATIONS: dict[str, dict[str, str]] = {
    "de": {
        "Question": "Frage",
        "Chart": "Diagramm",
        "Table": "Tabelle",
        "Details": "Details",
        "Assumptions": "Annahmen",
        "Query intent check": "Abfrageabsicht prüfen",
        "Inferred:": "Abgeleitet:",
        "Data quality notice:": "Hinweis zur Datenqualität:",
        "Tips for rephrasing your question": "Tipps zur Umformulierung",
        "Download PDF": "PDF herunterladen",
        "Send as email": "Per E-Mail senden",
        "Close email": "E-Mail schließen",
        "Recipient email address": "E-Mail-Adresse des Empfängers",
        "Send PDF report": "PDF-Bericht senden",
        "Sending...": "Wird gesendet...",
        "Enter a recipient email address.": "Bitte eine Empfänger-E-Mail-Adresse eingeben.",
        "Athena cost": "Athena-Kosten",
        "Data scanned": "Gescannte Daten",
        "Chart type": "Diagrammtyp",
        "Could not answer:": "Konnte nicht beantwortet werden:",
        "Request timed out. The query may be complex — try again.": "Zeitüberschreitung. Bitte erneut versuchen.",
        "Agent error:": "Agent-Fehler:",
        "Could not reach backend:": "Backend nicht erreichbar:",
        "EDP Analytics Report": "EDP-Analysebericht",
        "Summary": "Zusammenfassung",
        "DATA SNAPSHOT": "DATENAUSSCHNITT",
        "INTERNAL | CONFIDENTIAL": "INTERN | VERTRAULICH",
        "Confidential - Internal Use Only": "Vertraulich – Nur für den internen Gebrauch",
        "Source: Gold Layer · Athena": "Quelle: Gold-Schicht · Athena",
        "page_fmt": "Seite {n} von {m}",
        "DATA ENGINEER": "DATENINGENIEUR",
        "Generated:": "Erstellt:",
        "Period:": "Zeitraum:",
        "Periods Covered": "Abgedeckte Perioden",
        "Total Entries": "Einträge gesamt",
        "All Entries": "Alle Einträge",
        "Months": "Monate",
        "Total": "Gesamt",
        "All": "Alle",
        "total_metric_all_fmt": "Gesamt {metric} (Alle)",
        "Session": "Sitzung",
        "Started at": "Begonnen um",
        "questions_answered_fmt": "{n} Fragen beantwortet",
        "Start new session": "Neue Sitzung starten",
        "This will clear all questions. Are you sure?": "Dies löscht alle Fragen. Sind Sie sicher?",
        "Yes, clear": "Ja, löschen",
        "Cancel": "Abbrechen",
        "Export conversation (JSON)": "Gespräch exportieren (JSON)",
        "Import conversation (JSON)": "Gespräch importieren (JSON)",
        "History": "Verlauf",
        "restored_fmt": "{n} Gespräche wiederhergestellt.",
        "Import failed:": "Import fehlgeschlagen:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Stellen Sie Fragen zu Ihren Gold-Daten in jeder Sprache. Folgefragen erinnern sich an den Kontext.",
        "Try asking:": "Versuchen Sie zu fragen:",
        "SQL Query": "SQL-Abfrage",
        "Query Metadata": "Abfragemetadaten",
        "Query Intent Check": "Abfrageabsicht prüfen",
        "Athena cost:": "Athena-Kosten:",
        "Data scanned:": "Gescannte Daten:",
        "Chart type:": "Diagrammtyp:",
        "rephrase_tips": (
            "**Tipps zur Umformulierung:**\n"
            "- Zeitraum genau angeben: *'2024'*, *'letzte 6 Monate'*\n"
            "- Jeweils nur eine Kennzahl: Umsatz, Bestellungen, Lieferzeit\n"
            "- Aggregationsdimension nutzen: *'nach Land'*, *'nach Anbieter'*, *'nach Produkt'*\n"
            "- Offene Fragen vermeiden – Gold-Tabellen sind voraggregierte Zusammenfassungen"
        ),
    },
    "it": {
        "Question": "Domanda",
        "Chart": "Grafico",
        "Table": "Tabella",
        "Details": "Dettagli",
        "Assumptions": "Presupposti",
        "Query intent check": "Verifica intento query",
        "Inferred:": "Dedotto:",
        "Data quality notice:": "Avviso qualità dati:",
        "Tips for rephrasing your question": "Suggerimenti per riformulare",
        "Download PDF": "Scarica PDF",
        "Send as email": "Invia via email",
        "Close email": "Chiudi email",
        "Recipient email address": "Indirizzo email destinatario",
        "Send PDF report": "Invia report PDF",
        "Sending...": "Invio in corso...",
        "Enter a recipient email address.": "Inserire un indirizzo email destinatario.",
        "Athena cost": "Costo Athena",
        "Data scanned": "Dati analizzati",
        "Chart type": "Tipo di grafico",
        "Could not answer:": "Impossibile rispondere:",
        "Request timed out. The query may be complex — try again.": "Timeout richiesta. Riprovare.",
        "Agent error:": "Errore agente:",
        "Could not reach backend:": "Impossibile raggiungere il backend:",
        "EDP Analytics Report": "Report Analytics EDP",
        "Summary": "Riepilogo",
        "DATA SNAPSHOT": "ANTEPRIMA DATI",
        "INTERNAL | CONFIDENTIAL": "INTERNO | RISERVATO",
        "Confidential - Internal Use Only": "Riservato – Solo uso interno",
        "Source: Gold Layer · Athena": "Fonte: Gold Layer · Athena",
        "page_fmt": "Pagina {n} di {m}",
        "DATA ENGINEER": "DATA ENGINEER",
        "Generated:": "Generato:",
        "Period:": "Periodo:",
        "Periods Covered": "Periodi coperti",
        "Total Entries": "Voci totali",
        "All Entries": "Tutte le voci",
        "Months": "Mesi",
        "Total": "Totale",
        "All": "Tutte",
        "total_metric_all_fmt": "{metric} totale (Tutte)",
        "Session": "Sessione",
        "Started at": "Iniziato alle",
        "questions_answered_fmt": "{n} domande con risposta",
        "Start new session": "Avvia nuova sessione",
        "This will clear all questions. Are you sure?": "Verranno cancellate tutte le domande. Sei sicuro?",
        "Yes, clear": "Sì, cancella",
        "Cancel": "Annulla",
        "Export conversation (JSON)": "Esporta conversazione (JSON)",
        "Import conversation (JSON)": "Importa conversazione (JSON)",
        "History": "Cronologia",
        "restored_fmt": "Ripristinate {n} conversazioni.",
        "Import failed:": "Importazione fallita:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Fai domande sui tuoi dati Gold in qualsiasi lingua. Le domande di follow-up ricordano il contesto.",
        "Try asking:": "Prova a chiedere:",
        "SQL Query": "Query SQL",
        "Query Metadata": "Metadati query",
        "Query Intent Check": "Verifica intento query",
        "Athena cost:": "Costo Athena:",
        "Data scanned:": "Dati analizzati:",
        "Chart type:": "Tipo di grafico:",
        "rephrase_tips": (
            "**Suggerimenti per riformulare:**\n"
            "- Specifica il periodo: *'2024'*, *'ultimi 6 mesi'*\n"
            "- Chiedi una metrica alla volta: ricavi, ordini, giorni di consegna\n"
            "- Usa dimensioni di aggregazione: *'per paese'*, *'per corriere'*, *'per prodotto'*\n"
            "- Evita domande aperte – le tabelle Gold sono aggregazioni pre-calcolate"
        ),
    },
    "fr": {
        "Question": "Question",
        "Chart": "Graphique",
        "Table": "Tableau",
        "Details": "Détails",
        "Assumptions": "Hypothèses",
        "Query intent check": "Vérification de l'intention",
        "Inferred:": "Déduit :",
        "Data quality notice:": "Avis qualité des données :",
        "Tips for rephrasing your question": "Conseils de reformulation",
        "Download PDF": "Télécharger le PDF",
        "Send as email": "Envoyer par email",
        "Close email": "Fermer l'email",
        "Recipient email address": "Adresse email du destinataire",
        "Send PDF report": "Envoyer le rapport PDF",
        "Sending...": "Envoi en cours...",
        "Enter a recipient email address.": "Saisir une adresse email de destinataire.",
        "Athena cost": "Coût Athena",
        "Data scanned": "Données analysées",
        "Chart type": "Type de graphique",
        "Could not answer:": "Impossible de répondre :",
        "Request timed out. The query may be complex — try again.": "Délai dépassé. Veuillez réessayer.",
        "Agent error:": "Erreur de l'agent :",
        "Could not reach backend:": "Impossible d'atteindre le backend :",
        "EDP Analytics Report": "Rapport Analytics EDP",
        "Summary": "Résumé",
        "DATA SNAPSHOT": "APERÇU DES DONNÉES",
        "INTERNAL | CONFIDENTIAL": "INTERNE | CONFIDENTIEL",
        "Confidential - Internal Use Only": "Confidentiel – Usage interne uniquement",
        "Source: Gold Layer · Athena": "Source : Gold Layer · Athena",
        "page_fmt": "Page {n} sur {m}",
        "DATA ENGINEER": "DATA ENGINEER",
        "Generated:": "Généré le :",
        "Period:": "Période :",
        "Periods Covered": "Périodes couvertes",
        "Total Entries": "Entrées totales",
        "All Entries": "Toutes les entrées",
        "Months": "Mois",
        "Total": "Total",
        "All": "Toutes",
        "total_metric_all_fmt": "{metric} total (Toutes)",
        "Session": "Session",
        "Started at": "Démarré à",
        "questions_answered_fmt": "{n} questions auxquelles on a répondu",
        "Start new session": "Démarrer une nouvelle session",
        "This will clear all questions. Are you sure?": "Cela effacera toutes les questions. Êtes-vous sûr ?",
        "Yes, clear": "Oui, effacer",
        "Cancel": "Annuler",
        "Export conversation (JSON)": "Exporter la conversation (JSON)",
        "Import conversation (JSON)": "Importer la conversation (JSON)",
        "History": "Historique",
        "restored_fmt": "{n} échanges restaurés.",
        "Import failed:": "Importation échouée :",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Posez des questions sur vos données Gold dans n'importe quelle langue. Les questions de suivi se souviennent du contexte.",
        "Try asking:": "Essayez de demander :",
        "SQL Query": "Requête SQL",
        "Query Metadata": "Métadonnées de requête",
        "Query Intent Check": "Vérification de l'intention",
        "Athena cost:": "Coût Athena :",
        "Data scanned:": "Données analysées :",
        "Chart type:": "Type de graphique :",
        "rephrase_tips": (
            "**Conseils de reformulation :**\n"
            "- Précisez la période : *'2024'*, *'6 derniers mois'*\n"
            "- Posez une question par métrique : chiffre d'affaires, commandes, délai de livraison\n"
            "- Utilisez des dimensions d'agrégation : *'par pays'*, *'par transporteur'*, *'par produit'*\n"
            "- Évitez les questions ouvertes – les tables Gold sont des agrégats précalculés"
        ),
    },
    "es": {
        "Question": "Pregunta",
        "Chart": "Gráfico",
        "Table": "Tabla",
        "Details": "Detalles",
        "Assumptions": "Suposiciones",
        "Query intent check": "Verificación de intención",
        "Inferred:": "Inferido:",
        "Data quality notice:": "Aviso de calidad de datos:",
        "Tips for rephrasing your question": "Consejos para reformular",
        "Download PDF": "Descargar PDF",
        "Send as email": "Enviar por email",
        "Close email": "Cerrar email",
        "Recipient email address": "Dirección email del destinatario",
        "Send PDF report": "Enviar informe PDF",
        "Sending...": "Enviando...",
        "Enter a recipient email address.": "Introducir una dirección email de destinatario.",
        "Athena cost": "Costo Athena",
        "Data scanned": "Datos analizados",
        "Chart type": "Tipo de gráfico",
        "Could not answer:": "No se pudo responder:",
        "Request timed out. The query may be complex — try again.": "Tiempo de espera agotado. Inténtelo de nuevo.",
        "Agent error:": "Error del agente:",
        "Could not reach backend:": "No se pudo conectar al backend:",
        "EDP Analytics Report": "Informe Analytics EDP",
        "Summary": "Resumen",
        "DATA SNAPSHOT": "VISTA DE DATOS",
        "INTERNAL | CONFIDENTIAL": "INTERNO | CONFIDENCIAL",
        "Confidential - Internal Use Only": "Confidencial – Solo uso interno",
        "Source: Gold Layer · Athena": "Fuente: Gold Layer · Athena",
        "page_fmt": "Página {n} de {m}",
        "DATA ENGINEER": "INGENIERO DE DATOS",
        "Generated:": "Generado:",
        "Period:": "Período:",
        "Periods Covered": "Períodos cubiertos",
        "Total Entries": "Entradas totales",
        "All Entries": "Todas las entradas",
        "Months": "Meses",
        "Total": "Total",
        "All": "Todas",
        "total_metric_all_fmt": "{metric} total (Todas)",
        "Session": "Sesión",
        "Started at": "Iniciado a las",
        "questions_answered_fmt": "{n} preguntas respondidas",
        "Start new session": "Iniciar nueva sesión",
        "This will clear all questions. Are you sure?": "Esto borrará todas las preguntas. ¿Está seguro?",
        "Yes, clear": "Sí, borrar",
        "Cancel": "Cancelar",
        "Export conversation (JSON)": "Exportar conversación (JSON)",
        "Import conversation (JSON)": "Importar conversación (JSON)",
        "History": "Historial",
        "restored_fmt": "{n} conversaciones restauradas.",
        "Import failed:": "Importación fallida:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Haga preguntas sobre sus datos Gold en cualquier idioma. Las preguntas de seguimiento recuerdan el contexto.",
        "Try asking:": "Intente preguntar:",
        "SQL Query": "Consulta SQL",
        "Query Metadata": "Metadatos de consulta",
        "Query Intent Check": "Verificación de intención",
        "Athena cost:": "Costo Athena:",
        "Data scanned:": "Datos analizados:",
        "Chart type:": "Tipo de gráfico:",
        "rephrase_tips": (
            "**Consejos para reformular:**\n"
            "- Especifique el período: *'2024'*, *'últimos 6 meses'*\n"
            "- Pregunte por una métrica a la vez: ingresos, pedidos, tiempo de entrega\n"
            "- Use dimensiones de agregación: *'por país'*, *'por transportista'*, *'por producto'*\n"
            "- Evite preguntas abiertas – las tablas Gold son resúmenes pre-agregados"
        ),
    },
    "pt": {
        "Question": "Pergunta",
        "Chart": "Gráfico",
        "Table": "Tabela",
        "Details": "Detalhes",
        "Assumptions": "Premissas",
        "Query intent check": "Verificação de intenção",
        "Inferred:": "Inferido:",
        "Data quality notice:": "Aviso de qualidade de dados:",
        "Tips for rephrasing your question": "Dicas para reformular",
        "Download PDF": "Baixar PDF",
        "Send as email": "Enviar por email",
        "Close email": "Fechar email",
        "Recipient email address": "Endereço de email do destinatário",
        "Send PDF report": "Enviar relatório PDF",
        "Sending...": "Enviando...",
        "Enter a recipient email address.": "Inserir endereço de email do destinatário.",
        "Athena cost": "Custo Athena",
        "Data scanned": "Dados analisados",
        "Chart type": "Tipo de gráfico",
        "Could not answer:": "Não foi possível responder:",
        "Request timed out. The query may be complex — try again.": "Tempo esgotado. Tente novamente.",
        "Agent error:": "Erro do agente:",
        "Could not reach backend:": "Não foi possível contactar o backend:",
        "EDP Analytics Report": "Relatório Analytics EDP",
        "Summary": "Resumo",
        "DATA SNAPSHOT": "INSTANTÂNEO DE DADOS",
        "INTERNAL | CONFIDENTIAL": "INTERNO | CONFIDENCIAL",
        "Confidential - Internal Use Only": "Confidencial – Somente uso interno",
        "Source: Gold Layer · Athena": "Fonte: Gold Layer · Athena",
        "page_fmt": "Página {n} de {m}",
        "DATA ENGINEER": "ENGENHEIRO DE DADOS",
        "Generated:": "Gerado:",
        "Period:": "Período:",
        "Periods Covered": "Períodos cobertos",
        "Total Entries": "Entradas totais",
        "All Entries": "Todas as entradas",
        "Months": "Meses",
        "Total": "Total",
        "All": "Todas",
        "total_metric_all_fmt": "{metric} total (Todas)",
        "Session": "Sessão",
        "Started at": "Iniciado às",
        "questions_answered_fmt": "{n} perguntas respondidas",
        "Start new session": "Iniciar nova sessão",
        "This will clear all questions. Are you sure?": "Isso apagará todas as perguntas. Tem certeza?",
        "Yes, clear": "Sim, apagar",
        "Cancel": "Cancelar",
        "Export conversation (JSON)": "Exportar conversa (JSON)",
        "Import conversation (JSON)": "Importar conversa (JSON)",
        "History": "Histórico",
        "restored_fmt": "{n} conversas restauradas.",
        "Import failed:": "Importação falhou:",
        "Ask questions about your Gold data in any language. Follow-up questions remember prior context.": "Faça perguntas sobre seus dados Gold em qualquer idioma. Perguntas de acompanhamento lembram o contexto.",
        "Try asking:": "Tente perguntar:",
        "SQL Query": "Consulta SQL",
        "Query Metadata": "Metadados da consulta",
        "Query Intent Check": "Verificação de intenção",
        "Athena cost:": "Custo Athena:",
        "Data scanned:": "Dados analisados:",
        "Chart type:": "Tipo de gráfico:",
        "rephrase_tips": (
            "**Dicas para reformular:**\n"
            "- Especifique o período: *'2024'*, *'últimos 6 meses'*\n"
            "- Pergunte sobre uma métrica de cada vez: receita, pedidos, prazo de entrega\n"
            "- Use dimensões de agregação: *'por país'*, *'por transportadora'*, *'por produto'*\n"
            "- Evite perguntas abertas – as tabelas Gold são resumos pré-agregados"
        ),
    },
    # Missing PDF/KPI strings added to existing non-Latin-script entries
    "zh": {
        "DATA SNAPSHOT": "数据快照",
        "INTERNAL | CONFIDENTIAL": "内部 | 机密",
        "Confidential - Internal Use Only": "机密 – 仅供内部使用",
        "Source: Gold Layer · Athena": "来源：Gold 层 · Athena",
        "page_fmt": "第 {n} 页，共 {m} 页",
        "DATA ENGINEER": "数据工程师",
        "Generated:": "生成时间：",
        "Period:": "时间段：",
        "Periods Covered": "覆盖时段",
        "Total Entries": "条目总数",
        "All Entries": "所有条目",
        "Months": "个月",
        "Total": "总计",
        "All": "全部",
        "total_metric_all_fmt": "{metric} 合计（全部）",
    },
    "ja": {
        "DATA SNAPSHOT": "データスナップショット",
        "INTERNAL | CONFIDENTIAL": "社内 | 機密",
        "Confidential - Internal Use Only": "機密 – 社内使用限定",
        "Source: Gold Layer · Athena": "出典：Gold レイヤー · Athena",
        "page_fmt": "{n} / {m} ページ",
        "DATA ENGINEER": "データエンジニア",
        "Generated:": "生成日時：",
        "Period:": "期間：",
        "Periods Covered": "対象期間",
        "Total Entries": "合計エントリ数",
        "All Entries": "全エントリ",
        "Months": "ヶ月",
        "Total": "合計",
        "All": "全",
        "total_metric_all_fmt": "{metric} 合計（全体）",
    },
    "ko": {
        "DATA SNAPSHOT": "데이터 스냅샷",
        "INTERNAL | CONFIDENTIAL": "내부 | 기밀",
        "Confidential - Internal Use Only": "기밀 – 내부 전용",
        "Source: Gold Layer · Athena": "출처: Gold 레이어 · Athena",
        "page_fmt": "{n} / {m} 페이지",
        "DATA ENGINEER": "데이터 엔지니어",
        "Generated:": "생성:",
        "Period:": "기간:",
        "Periods Covered": "포함된 기간",
        "Total Entries": "총 항목 수",
        "All Entries": "모든 항목",
        "Months": "개월",
        "Total": "합계",
        "All": "전체",
        "total_metric_all_fmt": "{metric} 합계(전체)",
    },
    "ar": {
        "DATA SNAPSHOT": "لقطة البيانات",
        "INTERNAL | CONFIDENTIAL": "داخلي | سري",
        "Confidential - Internal Use Only": "سري – للاستخدام الداخلي فقط",
        "Source: Gold Layer · Athena": "المصدر: طبقة Gold · Athena",
        "page_fmt": "صفحة {n} من {m}",
        "DATA ENGINEER": "مهندس البيانات",
        "Generated:": "تم التوليد:",
        "Period:": "الفترة:",
        "Periods Covered": "الفترات المشمولة",
        "Total Entries": "إجمالي الإدخالات",
        "All Entries": "جميع الإدخالات",
        "Months": "أشهر",
        "Total": "الإجمالي",
        "All": "الكل",
        "total_metric_all_fmt": "إجمالي {metric} (الكل)",
    },
    "ru": {
        "DATA SNAPSHOT": "СНИМОК ДАННЫХ",
        "INTERNAL | CONFIDENTIAL": "ВНУТРЕННИЙ | КОНФИДЕНЦИАЛЬНО",
        "Confidential - Internal Use Only": "Конфиденциально – Только для внутреннего использования",
        "Source: Gold Layer · Athena": "Источник: Gold Layer · Athena",
        "page_fmt": "Страница {n} из {m}",
        "DATA ENGINEER": "ИНЖЕНЕР ДАННЫХ",
        "Generated:": "Создано:",
        "Period:": "Период:",
        "Periods Covered": "Охваченные периоды",
        "Total Entries": "Всего записей",
        "All Entries": "Все записи",
        "Months": "месяцев",
        "Total": "Всего",
        "All": "Все",
        "total_metric_all_fmt": "Всего {metric} (Все)",
    },
    "el": {
        "DATA SNAPSHOT": "ΣΤΙΓΜΙΌΤΥΠΟ ΔΕΔΟΜΈΝΩΝ",
        "INTERNAL | CONFIDENTIAL": "ΕΣΩΤΕΡΙΚΟ | ΕΜΠΙΣΤΕΥΤΙΚΟ",
        "Confidential - Internal Use Only": "Εμπιστευτικό – Μόνο για εσωτερική χρήση",
        "Source: Gold Layer · Athena": "Πηγή: Gold Layer · Athena",
        "page_fmt": "Σελίδα {n} από {m}",
        "DATA ENGINEER": "ΜΗΧΑΝΙΚΟΣ ΔΕΔΟΜΕΝΩΝ",
        "Generated:": "Δημιουργήθηκε:",
        "Period:": "Περίοδος:",
        "Periods Covered": "Καλυπτόμενες περίοδοι",
        "Total Entries": "Σύνολο εγγραφών",
        "All Entries": "Όλες οι εγγραφές",
        "Months": "μήνες",
        "Total": "Σύνολο",
        "All": "Όλες",
        "total_metric_all_fmt": "Σύνολο {metric} (Όλες)",
        "Summary": "Περίληψη",
    },
    "he": {
        "DATA SNAPSHOT": "תצלום נתונים",
        "INTERNAL | CONFIDENTIAL": "פנימי | סודי",
        "Confidential - Internal Use Only": "סודי – לשימוש פנימי בלבד",
        "Source: Gold Layer · Athena": "מקור: Gold Layer · Athena",
        "page_fmt": "עמוד {n} מתוך {m}",
        "DATA ENGINEER": "מהנדס נתונים",
        "Generated:": "נוצר:",
        "Period:": "תקופה:",
        "Periods Covered": "תקופות מכוסות",
        "Total Entries": "סך רשומות",
        "All Entries": "כל הרשומות",
        "Months": "חודשים",
        "Total": "סך הכל",
        "All": "הכל",
        "total_metric_all_fmt": "סך {metric} (הכל)",
        "Summary": "סיכום",
    },
    "th": {
        "DATA SNAPSHOT": "สแนปช็อตข้อมูล",
        "INTERNAL | CONFIDENTIAL": "ภายใน | ลับ",
        "Confidential - Internal Use Only": "ลับ – สำหรับใช้ภายในเท่านั้น",
        "Source: Gold Layer · Athena": "ที่มา: Gold Layer · Athena",
        "page_fmt": "หน้า {n} จาก {m}",
        "DATA ENGINEER": "วิศวกรข้อมูล",
        "Generated:": "สร้างเมื่อ:",
        "Period:": "ช่วงเวลา:",
        "Periods Covered": "ช่วงเวลาที่ครอบคลุม",
        "Total Entries": "รายการทั้งหมด",
        "All Entries": "รายการทั้งหมด",
        "Months": "เดือน",
        "Total": "รวม",
        "All": "ทั้งหมด",
        "total_metric_all_fmt": "{metric} รวม (ทั้งหมด)",
        "Summary": "สรุป",
    },
}

for _lc, _st in _EXTRA_TRANSLATIONS.items():
    if _lc in _UI_TRANSLATIONS:
        _UI_TRANSLATIONS[_lc].update(_st)
    else:
        _UI_TRANSLATIONS[_lc] = _st
del _lc, _st


# ── EDP column name translations (static dictionary for known Gold schema cols) ─
_COL_TRANSLATIONS: dict[str, dict[str, str]] = {
    "de": {
        "year_month": "Monat",
        "total_revenue": "Gesamtumsatz",
        "order_count": "Bestellungen",
        "unique_customers": "Eindeutige Kunden",
        "customer_id": "Kunden-ID",
        "first_name": "Vorname",
        "last_name": "Nachname",
        "country": "Land",
        "lifetime_value": "Lifetime-Wert",
        "segment": "Segment",
        "payment_method": "Zahlungsmethode",
        "payment_status": "Zahlungsstatus",
        "total_transactions": "Transaktionen",
        "successful_transactions": "Erfolgreiche Transaktionen",
        "success_rate_pct": "Erfolgsrate (%)",
        "total_payment_volume": "Zahlungsvolumen",
        "lost_revenue": "Entgangener Umsatz",
        "carrier_name": "Spediteur",
        "total_shipments": "Sendungen",
        "successful_deliveries": "Lieferungen",
        "avg_delivery_days": "Ø Liefertage",
        "category": "Kategorie",
        "brand": "Marke",
        "units_sold": "Verkaufte Einheiten",
        "revenue_rank": "Umsatz-Rang",
        "total_orders": "Bestellungen gesamt",
        "product_name": "Produkt",
        "revenue": "Umsatz",
        "avg_unit_revenue": "Ø Stückumsatz",
        "avg_order_value": "Ø Bestellwert",
    },
    "it": {
        "year_month": "Mese",
        "total_revenue": "Ricavo totale",
        "order_count": "Ordini",
        "unique_customers": "Clienti unici",
        "customer_id": "ID Cliente",
        "first_name": "Nome",
        "last_name": "Cognome",
        "country": "Paese",
        "lifetime_value": "Valore a vita",
        "segment": "Segmento",
        "payment_method": "Metodo di pagamento",
        "payment_status": "Stato pagamento",
        "total_transactions": "Transazioni",
        "successful_transactions": "Transazioni riuscite",
        "success_rate_pct": "Tasso di successo (%)",
        "total_payment_volume": "Volume pagamenti",
        "lost_revenue": "Ricavo perso",
        "carrier_name": "Corriere",
        "total_shipments": "Spedizioni",
        "successful_deliveries": "Consegne riuscite",
        "avg_delivery_days": "Giorni medi consegna",
        "category": "Categoria",
        "brand": "Marca",
        "units_sold": "Unità vendute",
        "revenue_rank": "Posizione",
        "total_orders": "Ordini totali",
        "product_name": "Prodotto",
        "revenue": "Ricavo",
        "avg_unit_revenue": "Ricavo unitario medio",
        "avg_order_value": "Valore medio ordine",
    },
    "fr": {
        "year_month": "Mois",
        "total_revenue": "Chiffre d'affaires",
        "order_count": "Commandes",
        "unique_customers": "Clients uniques",
        "customer_id": "ID Client",
        "first_name": "Prénom",
        "last_name": "Nom",
        "country": "Pays",
        "lifetime_value": "Valeur à vie",
        "segment": "Segment",
        "payment_method": "Mode de paiement",
        "payment_status": "Statut paiement",
        "total_transactions": "Transactions",
        "successful_transactions": "Transactions réussies",
        "success_rate_pct": "Taux de succès (%)",
        "total_payment_volume": "Volume paiements",
        "lost_revenue": "Revenus perdus",
        "carrier_name": "Transporteur",
        "total_shipments": "Expéditions",
        "successful_deliveries": "Livraisons réussies",
        "avg_delivery_days": "Jours livraison moy.",
        "category": "Catégorie",
        "brand": "Marque",
        "units_sold": "Unités vendues",
        "revenue_rank": "Classement",
        "total_orders": "Commandes totales",
        "product_name": "Produit",
        "revenue": "CA",
        "avg_unit_revenue": "CA unitaire moyen",
        "avg_order_value": "Valeur moy. commande",
    },
    "es": {
        "year_month": "Mes",
        "total_revenue": "Ingresos totales",
        "order_count": "Pedidos",
        "unique_customers": "Clientes únicos",
        "customer_id": "ID Cliente",
        "first_name": "Nombre",
        "last_name": "Apellido",
        "country": "País",
        "lifetime_value": "Valor de por vida",
        "segment": "Segmento",
        "payment_method": "Método de pago",
        "payment_status": "Estado del pago",
        "total_transactions": "Transacciones",
        "successful_transactions": "Transacciones exitosas",
        "success_rate_pct": "Tasa de éxito (%)",
        "total_payment_volume": "Volumen de pagos",
        "lost_revenue": "Ingresos perdidos",
        "carrier_name": "Transportista",
        "total_shipments": "Envíos",
        "successful_deliveries": "Entregas exitosas",
        "avg_delivery_days": "Días entrega prom.",
        "category": "Categoría",
        "brand": "Marca",
        "units_sold": "Unidades vendidas",
        "revenue_rank": "Clasificación",
        "total_orders": "Pedidos totales",
        "product_name": "Producto",
        "revenue": "Ingresos",
        "avg_unit_revenue": "Ingresos unit. medios",
        "avg_order_value": "Valor medio pedido",
    },
    "pt": {
        "year_month": "Mês",
        "total_revenue": "Receita total",
        "order_count": "Pedidos",
        "unique_customers": "Clientes únicos",
        "customer_id": "ID do Cliente",
        "first_name": "Nome",
        "last_name": "Sobrenome",
        "country": "País",
        "lifetime_value": "Valor vitalício",
        "segment": "Segmento",
        "payment_method": "Método de pagamento",
        "payment_status": "Status do pagamento",
        "total_transactions": "Transações",
        "successful_transactions": "Transações bem-sucedidas",
        "success_rate_pct": "Taxa de sucesso (%)",
        "total_payment_volume": "Volume de pagamentos",
        "lost_revenue": "Receita perdida",
        "carrier_name": "Transportadora",
        "total_shipments": "Envios",
        "successful_deliveries": "Entregas bem-sucedidas",
        "avg_delivery_days": "Dias entrega méd.",
        "category": "Categoria",
        "brand": "Marca",
        "units_sold": "Unidades vendidas",
        "revenue_rank": "Classificação",
        "total_orders": "Pedidos totais",
        "product_name": "Produto",
        "revenue": "Receita",
        "avg_unit_revenue": "Receita unit. média",
        "avg_order_value": "Valor médio pedido",
    },
    "zh": {
        "year_month": "月份",
        "total_revenue": "总收入",
        "order_count": "订单数",
        "unique_customers": "独立客户",
        "customer_id": "客户ID",
        "first_name": "名",
        "last_name": "姓",
        "country": "国家",
        "lifetime_value": "终身价值",
        "segment": "细分",
        "payment_method": "支付方式",
        "payment_status": "支付状态",
        "total_transactions": "交易数",
        "successful_transactions": "成功交易",
        "success_rate_pct": "成功率(%)",
        "total_payment_volume": "支付总额",
        "lost_revenue": "损失收入",
        "carrier_name": "承运商",
        "total_shipments": "发货数",
        "successful_deliveries": "成功交付",
        "avg_delivery_days": "平均交付天数",
        "category": "类别",
        "brand": "品牌",
        "units_sold": "销售数量",
        "revenue_rank": "收入排名",
        "total_orders": "总订单",
        "product_name": "产品",
        "revenue": "收入",
    },
    "ja": {
        "year_month": "年月",
        "total_revenue": "総収益",
        "order_count": "注文数",
        "unique_customers": "ユニーク顧客",
        "customer_id": "顧客ID",
        "first_name": "名",
        "last_name": "姓",
        "country": "国",
        "lifetime_value": "生涯価値",
        "segment": "セグメント",
        "payment_method": "支払方法",
        "payment_status": "支払状態",
        "total_transactions": "取引数",
        "successful_transactions": "成功取引",
        "success_rate_pct": "成功率(%)",
        "total_payment_volume": "支払総額",
        "lost_revenue": "損失収益",
        "carrier_name": "配送業者",
        "total_shipments": "出荷数",
        "successful_deliveries": "配送成功",
        "avg_delivery_days": "平均配送日数",
        "category": "カテゴリ",
        "brand": "ブランド",
        "units_sold": "販売数",
        "revenue_rank": "収益ランク",
        "total_orders": "総注文",
        "product_name": "商品",
        "revenue": "収益",
    },
    "ko": {
        "year_month": "월",
        "total_revenue": "총 매출",
        "order_count": "주문 수",
        "unique_customers": "고유 고객",
        "customer_id": "고객 ID",
        "first_name": "이름",
        "last_name": "성",
        "country": "국가",
        "lifetime_value": "생애 가치",
        "segment": "세그먼트",
        "payment_method": "결제 방법",
        "payment_status": "결제 상태",
        "total_transactions": "거래 수",
        "successful_transactions": "성공 거래",
        "success_rate_pct": "성공률(%)",
        "total_payment_volume": "결제 총액",
        "lost_revenue": "손실 매출",
        "carrier_name": "운송업체",
        "total_shipments": "배송 수",
        "successful_deliveries": "성공 배송",
        "avg_delivery_days": "평균 배송일",
        "category": "카테고리",
        "brand": "브랜드",
        "units_sold": "판매량",
        "revenue_rank": "매출 순위",
        "total_orders": "총 주문",
        "product_name": "제품",
        "revenue": "매출",
    },
    "ar": {
        "year_month": "الشهر",
        "total_revenue": "إجمالي الإيرادات",
        "order_count": "الطلبات",
        "unique_customers": "عملاء فريدون",
        "customer_id": "معرف العميل",
        "first_name": "الاسم الأول",
        "last_name": "اسم العائلة",
        "country": "الدولة",
        "lifetime_value": "القيمة الإجمالية",
        "segment": "الشريحة",
        "payment_method": "طريقة الدفع",
        "payment_status": "حالة الدفع",
        "total_transactions": "المعاملات",
        "successful_transactions": "المعاملات الناجحة",
        "success_rate_pct": "معدل النجاح (%)",
        "total_payment_volume": "حجم المدفوعات",
        "lost_revenue": "الإيرادات المفقودة",
        "carrier_name": "الناقل",
        "total_shipments": "الشحنات",
        "successful_deliveries": "التسليمات الناجحة",
        "avg_delivery_days": "متوسط أيام التسليم",
        "category": "الفئة",
        "brand": "العلامة التجارية",
        "units_sold": "الوحدات المباعة",
        "revenue_rank": "ترتيب الإيرادات",
        "total_orders": "إجمالي الطلبات",
        "product_name": "المنتج",
        "revenue": "الإيرادات",
    },
    "ru": {
        "year_month": "Месяц",
        "total_revenue": "Общая выручка",
        "order_count": "Заказы",
        "unique_customers": "Уникальные клиенты",
        "customer_id": "ID клиента",
        "first_name": "Имя",
        "last_name": "Фамилия",
        "country": "Страна",
        "lifetime_value": "Пожизненная ценность",
        "segment": "Сегмент",
        "payment_method": "Метод оплаты",
        "payment_status": "Статус оплаты",
        "total_transactions": "Транзакции",
        "successful_transactions": "Успешные транзакции",
        "success_rate_pct": "Успешность (%)",
        "total_payment_volume": "Объём платежей",
        "lost_revenue": "Потерянная выручка",
        "carrier_name": "Перевозчик",
        "total_shipments": "Отправления",
        "successful_deliveries": "Успешные доставки",
        "avg_delivery_days": "Ср. дней доставки",
        "category": "Категория",
        "brand": "Бренд",
        "units_sold": "Продано единиц",
        "revenue_rank": "Рейтинг выручки",
        "total_orders": "Всего заказов",
        "product_name": "Продукт",
        "revenue": "Выручка",
    },
}


def _translate_col(col: str, lang: str) -> str:
    """Translate an EDP Gold-schema column name into the report language.

    Falls back to title-cased English with underscores replaced by spaces.
    """
    if lang == "en":
        return col.replace("_", " ").title()
    translated = _COL_TRANSLATIONS.get(lang, {}).get(col)
    if translated:
        return translated
    # Fallback: title-case the column name without underscores
    return col.replace("_", " ").title()


# Localised month abbreviations for PDF period labels and chart axis ticks.
_MONTH_NAMES: dict[str, list[str]] = {
    "en": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    "de": [
        "Jan.",
        "Feb.",
        "März",
        "Apr.",
        "Mai",
        "Jun.",
        "Jul.",
        "Aug.",
        "Sep.",
        "Okt.",
        "Nov.",
        "Dez.",
    ],
    "it": ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu", "Lug", "Ago", "Set", "Ott", "Nov", "Dic"],
    "fr": [
        "Jan.",
        "Fév.",
        "Mars",
        "Avr.",
        "Mai",
        "Juin",
        "Juil.",
        "Aoû.",
        "Sep.",
        "Oct.",
        "Nov.",
        "Déc.",
    ],
    "es": [
        "Ene.",
        "Feb.",
        "Mar.",
        "Abr.",
        "May.",
        "Jun.",
        "Jul.",
        "Ago.",
        "Sep.",
        "Oct.",
        "Nov.",
        "Dic.",
    ],
    "pt": [
        "Jan.",
        "Fev.",
        "Mar.",
        "Abr.",
        "Mai.",
        "Jun.",
        "Jul.",
        "Ago.",
        "Set.",
        "Out.",
        "Nov.",
        "Dez.",
    ],
    "zh": ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    "ja": ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"],
    "ko": ["1월", "2월", "3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월"],
    "ru": [
        "янв.",
        "фев.",
        "март",
        "апр.",
        "май",
        "июн.",
        "июл.",
        "авг.",
        "сен.",
        "окт.",
        "ноя.",
        "дек.",
    ],
    "ar": [
        "يناير",
        "فبراير",
        "مارس",
        "أبريل",
        "مايو",
        "يونيو",
        "يوليو",
        "أغسطس",
        "سبتمبر",
        "أكتوبر",
        "نوفمبر",
        "ديسمبر",
    ],
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


def _extract_kpi_tiles(
    columns: list[str], rows: list[dict], lang: str = "en"
) -> list[tuple[str, str, str, str]]:
    """Return up to 3 (metric_label, value, sub_label, badge) KPI tuples from query results.

    badge is a MoM/period change string like '+12.3% vs prior' or '' when not applicable.
    sub_label for time-series results shows a human-readable period value (e.g. 'Apr 2025').
    """
    if not rows or not columns:
        return []

    # Defined early so the classification loop can use it.
    _time_col_hints = {"year", "month", "date", "week", "quarter", "period"}

    numeric_cols: list[str] = []
    cat_cols: list[str] = []
    for col in columns:
        # Time-dimension columns are always categorical, even when their values
        # look numeric (e.g. order_year=2024, order_month=4).
        if any(hint in col.lower() for hint in _time_col_hints):
            cat_cols.append(col)
            continue
        raw = str(rows[0].get(col, "") or "")
        try:
            float(raw.replace(",", "").replace("$", ""))
            numeric_cols.append(col)
        except ValueError:
            cat_cols.append(col)

    def _fmt(val: str, col: str = "") -> str:
        try:
            f = float(str(val).replace(",", "").replace("€", "").replace("$", ""))
            _mon = col and any(h in col.lower() for h in _kpi_priority)
            prefix = "€" if _mon else ""
            if f == int(f):
                return f"{prefix}{int(f):,}"
            return f"{prefix}{f:,.2f}"
        except (ValueError, TypeError):
            return str(val)

    def _fmt_period(val: str) -> str:
        """Format '2025-04' or '2025-04-01' → 'Apr 2025'. Falls back to val."""
        _months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        m = re.match(r"^(\d{4})-(\d{2})(?:-\d{2})?$", str(val).strip())
        if m:
            mo = int(m.group(2)) - 1
            if 0 <= mo <= 11:
                return f"{_months[mo]} {m.group(1)}"
        return val

    # Priority hints for picking the primary KPI metric — mirrors charts.py logic.
    _kpi_priority = {
        "revenue",
        "amount",
        "sales",
        "profit",
        "income",
        "spend",
        "price",
        "value",
        "payment",
    }
    _kpi_counts = {"count", "customers", "users", "visitors", "quantity", "qty"}
    _kpi_any = {
        "revenue",
        "total",
        "amount",
        "sales",
        "profit",
        "sum",
        "value",
        "orders",
        "avg",
        "average",
    }
    _rank_exact = frozenset(
        {"rank", "position", "pos", "row_num", "row_number", "rn", "ntile", "dense_rank"}
    )

    def _is_rank_col(c: str) -> bool:
        cl = c.lower()
        return (
            cl in _rank_exact
            or cl.endswith("_rank")
            or cl.endswith("_position")
            or cl.endswith("_pos")
        )

    def _pick_metric(cols: list[str]) -> str:
        non_rank = [c for c in cols if not _is_rank_col(c)]
        cands = non_rank if non_rank else cols
        # Scan forward so earlier columns (e.g. total_revenue) beat later ones
        # (e.g. avg_order_value) when both match a priority hint.
        for c in cands:
            if any(h in c.lower() for h in _kpi_priority):
                return c
        for c in cands:
            if any(h in c.lower() for h in _kpi_any) and not any(
                h in c.lower() for h in _kpi_counts
            ):
                return c
        for c in cands:
            if any(h in c.lower() for h in _kpi_any):
                return c
        return cands[-1]

    tiles: list[tuple[str, str, str, str]] = []

    if cat_cols and numeric_cols:
        metric_col = _pick_metric(numeric_cols)
        cat_col = cat_cols[0]
        metric_label = _translate_col(metric_col, lang)

        # Detect companion year/month cols for integer-encoded time series
        # (e.g. order_year=2024 + order_month=4 → synthesize "2024-04-01").
        _year_col = next((c for c in cat_cols if "year" in c.lower()), None)
        _month_col = next((c for c in cat_cols if "month" in c.lower() and c != _year_col), None)

        def _period_str(row: dict) -> str:
            """Return a YYYY-MM-01 string from split year/month cols, else the cat_col value."""
            if _year_col and _month_col:
                y = str(row.get(_year_col, "") or "")
                mo = str(row.get(_month_col, "") or "")
                if y.isdigit() and mo.isdigit():
                    return f"{y}-{mo.zfill(2)}-01"
            return str(row.get(cat_col, ""))

        is_time_cat = any(hint in cat_col.lower() for hint in _time_col_hints)
        # For time-series results, tile 0 shows the most-recent period (last row)
        # so the MoM badge is consistent with the value displayed.
        display_row = rows[-1] if is_time_cat else rows[0]
        if is_time_cat:
            top_cat = _fmt_period(_period_str(display_row))
        else:
            top_raw = str(display_row.get(cat_col, ""))
            # Replace underscores and title-case lowercase DB values.
            cleaned_raw = top_raw.replace("_", " ")
            top_cat = cleaned_raw.title() if top_raw == top_raw.lower() else cleaned_raw
        top_val = _fmt(str(display_row.get(metric_col, "")), col=metric_col)

        # MoM badge: most-recent vs prior period for time-series results.
        badge = ""
        if is_time_cat and len(rows) >= 2:
            try:
                cur = float(str(rows[-1].get(metric_col, 0) or 0).replace(",", ""))
                prv = float(str(rows[-2].get(metric_col, 0) or 0).replace(",", ""))
                if prv != 0:
                    pct = (cur - prv) / abs(prv) * 100
                    sign = "+" if pct >= 0 else ""
                    badge = f"{sign}{pct:.1f}% vs prior"
            except (ValueError, TypeError):
                pass

        tiles.append((metric_label, top_val, top_cat, badge))

        # Tile 2: entry count with a localised time-aware plural label.
        if is_time_cat:
            col_l = cat_col.lower()
            # Prefer "Months" when there is a companion month column (e.g. the
            # primary cat_col is order_year but order_month is also present).
            if "month" in col_l or _month_col:
                cat_plural = _t("Months", lang)
            elif "week" in col_l:
                cat_plural = "Weeks"
            elif "quarter" in col_l:
                cat_plural = "Quarters"
            elif "year" in col_l:
                cat_plural = "Years"
            else:
                cat_plural = "Periods"
        else:
            cat_label = _translate_col(cat_col, lang)
            if lang == "en":
                if cat_label.endswith("y"):
                    cat_plural = cat_label[:-1] + "ies"
                elif cat_label.endswith("s"):
                    cat_plural = cat_label
                else:
                    cat_plural = cat_label + "s"
            else:
                # Non-English: never append "s" — pluralisation rules differ
                cat_plural = cat_label
        tile2_label = _t("Periods Covered", lang) if is_time_cat else _t("Total Entries", lang)
        tiles.append((tile2_label, str(len(rows)), cat_plural, ""))

        # Tile 3: aggregate total with period range sub-label for time-series.
        try:
            total = sum(float(str(r.get(metric_col, 0) or 0).replace(",", "")) for r in rows)
            # Tile 3 label: use the BASE column name (strip "total_" prefix) so we
            # don't get "Ricavo totale totale" when the translated name already
            # includes "total"/"totale"/etc.
            _base_col = re.sub(r"^total_", "", metric_col, flags=re.IGNORECASE)
            _base_col = re.sub(r"_total$", "", _base_col, flags=re.IGNORECASE)
            _base_label = (
                _translate_col(_base_col, lang) if _base_col != metric_col else metric_label
            )
            tile3_label = f"{_t('Total', lang)} {_base_label} ({_t('All', lang)})"
            if is_time_cat and len(rows) >= 2:
                first_fmt = _fmt_period(_period_str(rows[0]))
                last_fmt = _fmt_period(_period_str(rows[-1]))
                tile3_sub = f"{first_fmt} \u2013 {last_fmt}"
            else:
                tile3_sub = _t("All Entries", lang)
            tiles.append((tile3_label, _fmt(str(total), col=metric_col), tile3_sub, ""))
        except (ValueError, TypeError):
            pass
    elif numeric_cols:
        # No categorical column — show top 3 numeric values from row 0.
        for col in numeric_cols[:3]:
            val = _fmt(str(rows[0].get(col, "")))
            label = _translate_col(col, lang)
            tiles.append((label, val, "", ""))
    else:
        tiles.append(
            (
                _t("Total Results", lang)
                if _t("Total Results", lang) != "Total Results"
                else "Total Results",
                str(len(rows)),
                "",
                "",
            )
        )

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
    # 0. Replace em dash (—) and pseudo-em-dash ( -- ) with a comma+space.
    text = re.sub(r"\s*\u2014\s*", ", ", text)
    text = re.sub(r"\s+--\s+", ", ", text)
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
    """Draw the EDP enterprise logo mark at position (x, y).

    Design: three ascending data bars on a thick baseline inside an olive square.
    Three bars (vs four) gives each bar more width so the silhouette reads
    clearly at the small 12 mm print size. No accent line — keeps it clean.

    Bar heights (as fraction of usable area): 0.40, 0.70, 1.00

    Args:
        pdf: fpdf2 FPDF instance.
        x, y: Top-left corner of the mark in mm.
        size: Side length of the square in mm.
    """
    # ── Background ──────────────────────────────────────────────────────────
    pdf.set_fill_color(75, 83, 32)  # #4B5320 army olive
    pdf.rect(x, y, size, size, style="F")

    # ── Layout constants ────────────────────────────────────────────────────
    pad_x = size * 0.18  # horizontal inset on each side
    pad_top = size * 0.14  # top inset
    pad_bot = size * 0.20  # bottom inset (above baseline)

    usable_w = size - 2 * pad_x
    usable_h = size - pad_top - pad_bot

    n_bars = 3
    gap = usable_w * 0.12  # gap between bars (12 % of usable width)
    bar_w = (usable_w - (n_bars - 1) * gap) / n_bars

    baseline_y = y + size - pad_bot
    bar_fractions = [0.40, 0.70, 1.00]  # height ratios — clear step progression

    pdf.set_fill_color(255, 255, 255)

    # ── Ascending bars ───────────────────────────────────────────────────────
    for i, frac in enumerate(bar_fractions):
        bh = usable_h * frac
        bx = x + pad_x + i * (bar_w + gap)
        by = baseline_y - bh
        pdf.rect(bx, by, bar_w, bh, style="F")

    # ── Baseline (thick white rule below bars) ───────────────────────────────
    baseline_h = size * 0.06
    pdf.set_fill_color(255, 255, 255)
    pdf.rect(x + pad_x, baseline_y, usable_w, baseline_h, style="F")

    # Reset fill colour
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
    chart_type: str = "",
    lang: str = "en",
) -> bytes:
    """Build a single-page stakeholder PDF report.

    Header (logo + name + classification) | Question | KPI tiles |
    Chart | Key Finding | Footer.
    Methodology is written to the engineer log, not the PDF.
    """
    import datetime
    import io
    import zoneinfo as _zi

    from fpdf import FPDF

    lang = _detect_language(question)
    columns: list[str] = json.loads(columns_json) if columns_json else []
    rows: list[dict] = json.loads(rows_json) if rows_json else []

    _now = datetime.datetime.now(_zi.ZoneInfo("Europe/Berlin"))
    _mo_idx = _now.month - 1  # 0-indexed
    _month_str = _MONTH_NAMES.get(lang, _MONTH_NAMES["en"])[_mo_idx]
    generated_str = f"{_now.day:02d} {_month_str} {_now.year}, {_now.strftime('%H:%M %Z')}"

    # ── Font selection ──────────────────────────────────────────────────────
    # CJK candidates: explicit paths + recursive glob + fc-list discovery.
    _NOTO_CJK_PATHS = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
    ] + _glob.glob("/usr/share/fonts/**/*[Nn]oto*[Cc][Jj][Kk]*", recursive=True)

    # DejaVu candidates across Debian/Ubuntu/Alpine layouts.
    _DEJAVU_REG_PATHS = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    _DEJAVU_BOLD_PATHS = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    ]

    # Resolve font name before instantiating _PDFReport (header() needs it).
    _font_name_resolved = "Helvetica"
    _font_map: dict[str, str] = {}

    _cjk_path_to_use: str | None = None
    if lang in ("zh", "ja", "ko"):
        for cjk_path in _NOTO_CJK_PATHS:
            if os.path.exists(cjk_path):
                _cjk_path_to_use = cjk_path
                _font_name_resolved = "NotoSansCJK"
                break
        # Fallback: ask fontconfig for any CJK-capable font on the system.
        if not _cjk_path_to_use:
            try:
                import subprocess as _sp

                _fc = _sp.run(
                    ["fc-list", "--format=%{file}\n"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for _fp in _fc.stdout.strip().split("\n"):
                    _fp = _fp.strip()
                    if (
                        _fp
                        and os.path.exists(_fp)
                        and _fp.endswith((".ttf", ".otf", ".ttc"))
                        and any(
                            x in _fp.lower()
                            for x in ["noto", "cjk", "han", "wqy", "unifont", "droid"]
                        )
                    ):
                        _cjk_path_to_use = _fp
                        _font_name_resolved = "NotoSansCJK"
                        break
            except Exception:
                pass

    if _font_name_resolved == "Helvetica":
        _dv_reg = next((p for p in _DEJAVU_REG_PATHS if os.path.exists(p)), None)
        _dv_bold = next((p for p in _DEJAVU_BOLD_PATHS if os.path.exists(p)), None)
        if _dv_reg:
            _font_name_resolved = "DejaVu"
            _font_map["reg"] = _dv_reg
            _font_map["bold"] = _dv_bold or _dv_reg

    font_name = _font_name_resolved
    _is_cjk_font = font_name == "NotoSansCJK"

    # ── Text sanitizer — prevents character-support errors on Helvetica ──────
    # When font registration fails and we fall back to Helvetica, non-Latin-1
    # characters (en-dash, smart quotes, CJK) cause fpdf2 to raise. This
    # sanitizer replaces the most common offenders. For CJK text with a CJK
    # font, no substitution is needed.
    def _safe(text: str) -> str:
        if _is_cjk_font:
            return text
        # Use explicit Unicode escapes so the keys are unambiguous regardless
        # of how the source file is saved — literal glyph chars can silently
        # become the wrong codepoint in some editors.
        _r = {
            "–": "-",
            "—": "--",
            "‘": "'",
            "’": "'",
            "“": '"',
            "”": '"',
            "…": "...",
            "•": "-",
            "·": ".",
            "‒": "-",
            "―": "--",
        }
        for ch, rep in _r.items():
            text = text.replace(ch, rep)
        if font_name == "Helvetica":
            # Keep printable Latin-1 (U+0020–U+00FF) and the Euro sign (U+20AC),
            # which FPDF2's built-in Helvetica glyph set supports. Replace
            # everything else (CJK, en-dash survivors, etc.) with '?'.
            text = "".join(ch if (0x20 <= ord(ch) <= 0xFF or ch == "€") else "?" for ch in text)
        return text

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
            self.cell(55, 4, _safe(_t("DATA ENGINEER", self._lang)))

            # Centre: report title
            report_title = _safe(_t("EDP Analytics Report", self._lang))
            self.set_font(self._fn, "B", 10)
            self.set_text_color(255, 255, 255)
            title_w = 70.0
            self.set_xy(self.w / 2 - title_w / 2, 9)
            self.cell(title_w, 6, report_title, align="C")

            # Right: classification + generation timestamp
            self.set_font(self._fn, "", 7)
            self.set_text_color(180, 210, 230)
            self.set_xy(self.w - 62, 7)
            self.cell(57, 5, _safe(_t("INTERNAL | CONFIDENTIAL", self._lang)), align="R")
            self.set_xy(self.w - 62, 13)
            self.set_font(self._fn, "", 6)
            self.cell(57, 4, _safe(f"{_t('Generated:', self._lang)} {self._gen}"), align="R")

            # Reset colour; content starts at y=28
            self.set_text_color(0, 0, 0)
            self.set_y(28)

        def footer(self) -> None:
            self.set_y(-13)
            col_w = self.epw / 3
            self.set_font(self._fn, "", 7)
            self.set_text_color(148, 163, 184)
            self.set_x(self.l_margin)
            self.cell(col_w, 5, _safe(_t("Source: Gold Layer · Athena", self._lang)), align="L")
            self.cell(
                col_w, 5, _safe(_t("Confidential - Internal Use Only", self._lang)), align="C"
            )
            _page_fmt = _t("page_fmt", self._lang)
            if _page_fmt == "page_fmt":
                _page_str = f"Page {self.page_no()} of {{nb}}"
            else:
                _page_str = _page_fmt.replace("{n}", str(self.page_no())).replace("{m}", "{nb}")
            self.cell(col_w, 5, _safe(_page_str), align="R")
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

    # ── Local helpers (PDF scope) ─────────────────────────────────────────────
    _PDF_MONTHS = _MONTH_NAMES.get(lang, _MONTH_NAMES["en"])
    _PDF_TIME_HINTS = {"year", "month", "date", "week", "quarter", "period"}
    _PDF_MON_HINTS = {
        "revenue",
        "amount",
        "sales",
        "profit",
        "spend",
        "cost",
        "price",
        "income",
        "value",
        "payment",
        "volume",
        "lifetime",
    }

    def _fmt_period_pdf(val: str) -> str:
        m = re.match(r"^(\d{4})-(\d{2})(?:-\d{2})?$", str(val).strip())
        if m:
            mo = int(m.group(2)) - 1
            if 0 <= mo <= 11:
                return f"{_PDF_MONTHS[mo]} {m.group(1)}"
        return val

    def _is_mon_col(col: str) -> bool:
        return any(h in col.lower() for h in _PDF_MON_HINTS)

    def _fmt_snap(val: str, col: str) -> str:
        """Format a cell value for the data snapshot table."""
        try:
            f = float(str(val).replace(",", "").replace("€", "").replace("$", ""))
            if _is_mon_col(col):
                return f"€{f:,.0f}"
            if f == int(f):
                return f"{int(f):,}"
            return f"{f:,.2f}"
        except (ValueError, TypeError):
            s = str(val)
            # Replace underscores and title-case lowercase database values.
            cleaned = s.replace("_", " ")
            return cleaned.title() if s == s.lower() else cleaned

    # Classify columns for snapshot table and period detection.
    _pdf_numeric: list[str] = []
    _pdf_cat: list[str] = []
    for _col in columns:
        _raw = str(rows[0].get(_col, "") or "") if rows else ""
        try:
            float(_raw.replace(",", "").replace("$", ""))
            _pdf_numeric.append(_col)
        except ValueError:
            _pdf_cat.append(_col)

    # Detect period range from any time-dimension column.
    period_label = ""
    if rows:
        for _col in _pdf_cat:
            if any(h in _col.lower() for h in _PDF_TIME_HINTS):
                first_v = str(rows[0].get(_col, "")).strip()
                last_v = str(rows[-1].get(_col, "")).strip()
                if re.match(r"^\d{4}", first_v):
                    first_fmt = _fmt_period_pdf(first_v)
                    last_fmt = _fmt_period_pdf(last_v)
                    if first_fmt and first_fmt != last_fmt:
                        period_label = f"{first_fmt} \u2013 {last_fmt}"
                    elif first_fmt:
                        period_label = first_fmt
                    break

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 1 — Question · Period · KPI tiles · Chart · Summary · Snapshot
    # ════════════════════════════════════════════════════════════════════════
    pdf.add_page()
    W = pdf.epw  # effective page width inside margins

    # ── Question ─────────────────────────────────────────────────────────────
    pdf.set_font(font_name, "B", 11)
    pdf.set_text_color(75, 83, 32)
    pdf.cell(W, 5, _safe(_t("Question", lang)).upper(), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
    pdf.ln(3)
    pdf.set_font(font_name, "B", 13)
    pdf.set_text_color(15, 23, 42)
    pdf.multi_cell(W, 7, _safe(question), align="L")
    pdf.ln(3)

    # ── Period coverage line ─────────────────────────────────────────────────
    if period_label:
        period_prefix = _t("Period:", lang)
        pdf.set_font(font_name, "", 8)
        pdf.set_text_color(100, 116, 139)
        pdf.cell(W, 5, _safe(f"{period_prefix} {period_label}"), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)
    else:
        pdf.ln(5)

    # ── KPI tiles (26 mm tall — extra row for MoM badge on tile 0) ──────────
    kpi_tiles = _extract_kpi_tiles(columns, rows, lang)
    if kpi_tiles:
        tile_w = W / len(kpi_tiles)
        tile_h = 26.0
        tile_y = pdf.get_y()
        for i, (metric_lbl, value, sub_lbl, badge) in enumerate(kpi_tiles):
            tx = pdf.l_margin + i * tile_w
            # Tile background
            pdf.set_fill_color(240, 247, 255)
            pdf.rect(tx, tile_y, tile_w - 2, tile_h, style="F")
            # Top olive accent stripe (3 mm)
            pdf.set_fill_color(75, 83, 32)
            pdf.rect(tx, tile_y, tile_w - 2, 3, style="F")
            # Metric label
            pdf.set_xy(tx + 3, tile_y + 4)
            pdf.set_font(font_name, "", 7)
            pdf.set_text_color(75, 83, 32)
            pdf.cell(tile_w - 5, 4, _safe(metric_lbl).upper(), align="L")
            # Value
            pdf.set_xy(tx + 3, tile_y + 9)
            pdf.set_font(font_name, "B", 13)
            pdf.set_text_color(15, 23, 42)
            pdf.cell(tile_w - 5, 7, _safe(value), align="L")
            # Sub-label — _safe() converts en-dash/smart-quotes to ASCII for Helvetica.
            pdf.set_xy(tx + 3, tile_y + 17)
            pdf.set_font(font_name, "", 7)
            pdf.set_text_color(75, 83, 32)
            pdf.cell(tile_w - 5, 4, _safe(sub_lbl), align="L")
            # MoM badge (tile 0 only, when available)
            if badge:
                positive = badge.startswith("+")
                r, g, b = (34, 197, 94) if positive else (239, 68, 68)
                pdf.set_xy(tx + 3, tile_y + 21)
                pdf.set_font(font_name, "B", 7)
                pdf.set_text_color(r, g, b)
                pdf.cell(tile_w - 5, 4, _safe(badge), align="L")

        pdf.set_y(tile_y + tile_h + 5)
        pdf.set_text_color(0, 0, 0)
        pdf.set_fill_color(255, 255, 255)

    # ── Chart image ──────────────────────────────────────────────────────────
    # Skip the PNG for table-type results: the matplotlib table is redundant
    # because the DATA SNAPSHOT below already shows the same data in a cleaner
    # format, and embedding it consumes most of the page and truncates the summary.
    if png_b64 and chart_type != "table":
        png_bytes = _b64.b64decode(png_b64)
        # Cap chart height so there is always room below for the summary and
        # data snapshot.  Reserve: snapshot(38) + summary heading+gap(13) +
        # 3 summary lines minimum(21) + caption+gap(8) = 80 mm.
        _chart_reserve = 105.0
        _safe_bottom_pre = pdf.h - 18.0
        _chart_max_h = max(40.0, _safe_bottom_pre - pdf.get_y() - _chart_reserve)
        # Read PNG natural dimensions from the IHDR chunk (bytes 16-23).
        _png_w_px = int.from_bytes(png_bytes[16:20], "big")
        _png_h_px = int.from_bytes(png_bytes[20:24], "big")
        _natural_h = W * (_png_h_px / _png_w_px) if _png_w_px > 0 else _chart_max_h
        if _natural_h > _chart_max_h:
            # Scale proportionally to fit within the height cap; centre horizontally.
            _embed_w = W * (_chart_max_h / _natural_h)
            pdf.image(
                io.BytesIO(png_bytes),
                x=pdf.l_margin + (W - _embed_w) / 2,
                w=_embed_w,
                h=_chart_max_h,
            )
        else:
            pdf.image(io.BytesIO(png_bytes), x=pdf.l_margin, w=W)
        # Data source caption flush-right below chart.
        pdf.set_font(font_name, "", 7)
        pdf.set_text_color(148, 163, 184)
        pdf.cell(W, 4, "Source: Gold Layer \u00b7 Athena", align="R", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)

    # ── Summary (insight) — olive left-accent bar ─────────────────────────────
    pdf_insight = re.sub(r"\*{1,3}([^*\n]+)\*{1,3}", r"\1", insight)
    pdf_insight = pdf_insight.replace("\r\n", "\n").replace("\r", "\n")
    pdf_insight = _safe(pdf_insight)
    pdf.set_font(font_name, "B", 11)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(W, 5, _safe(_t("Summary", lang)).upper(), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(226, 232, 240)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
    pdf.ln(4)
    pdf.set_font(font_name, "", 11)
    pdf.set_text_color(30, 41, 59)

    # Summary flows freely with auto page break — long insights continue on
    # page 2 rather than being truncated. The olive accent bar is clipped at
    # the page bottom to avoid crossing a page boundary.
    _line_h = 7.0
    _cell_w = W - 6
    pdf.set_auto_page_break(True, margin=18)
    y_before = pdf.get_y()
    _page_before = pdf.page_no()
    pdf.set_x(pdf.l_margin + 6)
    pdf.multi_cell(_cell_w, _line_h, pdf_insight, align="L")
    y_after = pdf.get_y()

    # Draw the olive accent bar only on the starting page (can't span pages).
    pdf.set_fill_color(75, 83, 32)
    _bar_end = (pdf.h - 18) if pdf.page_no() > _page_before else y_after
    if _bar_end > y_before:
        _saved_page = pdf.page_no()
        pdf.page = _page_before
        pdf.rect(pdf.l_margin, y_before, 3, _bar_end - y_before, style="F")
        pdf.page = _saved_page
    pdf.set_fill_color(255, 255, 255)

    # ── Data Snapshot — top 5 rows compact table ──────────────────────────────
    # Exclude surrogate key columns (_id suffix) — they are meaningless to stakeholders.
    _pdf_cat_clean = [c for c in _pdf_cat if not c.lower().endswith("_id") and c.lower() != "id"]
    _pdf_num_clean = [
        c for c in _pdf_numeric if not c.lower().endswith("_id") and c.lower() != "id"
    ]
    # For mixed results: first categorical column + up to 3 numeric columns.
    # For all-categorical results: show up to 4 columns so multi-column results
    # aren't reduced to a single column.
    if _pdf_num_clean:
        snap_cols = (_pdf_cat_clean[:1] if _pdf_cat_clean else []) + _pdf_num_clean[:3]
    else:
        snap_cols = _pdf_cat_clean[:4]
    # Sort snapshot rows by first time-dimension column ascending (ISO YYYY-MM
    # sorts correctly as a string) so they appear in chronological order.
    snap_rows_unsorted = rows[:5]
    if snap_cols and any(h in snap_cols[0].lower() for h in _PDF_TIME_HINTS):
        try:
            snap_rows_unsorted = sorted(
                snap_rows_unsorted, key=lambda r: str(r.get(snap_cols[0], "") or "")
            )
        except Exception:
            pass
    snap_rows = snap_rows_unsorted
    remaining_mm = pdf.h - pdf.get_y() - pdf.b_margin
    if snap_cols and snap_rows and remaining_mm >= 38:
        pdf.ln(7)
        pdf.set_font(font_name, "B", 11)
        pdf.set_text_color(100, 116, 139)
        pdf.cell(W, 5, _safe(_t("DATA SNAPSHOT", lang)), new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(226, 232, 240)
        pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + W, pdf.get_y())
        pdf.ln(3)

        col_w_s = W / len(snap_cols)
        row_h_s = 6.5

        # Header row
        hdr_y = pdf.get_y()
        pdf.set_fill_color(75, 83, 32)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font(font_name, "B", 8)
        for j, sc in enumerate(snap_cols):
            pdf.set_xy(pdf.l_margin + j * col_w_s, hdr_y)
            pdf.cell(col_w_s, row_h_s, _safe(_translate_col(sc, lang)), fill=True)
        pdf.set_y(hdr_y + row_h_s)

        # Data rows — auto page break is off so cells never split mid-row.
        # Use an explicit safe bottom (18 mm above page bottom = above footer).
        # pdf.b_margin becomes 0 after set_auto_page_break(False) in fpdf2,
        # so we cannot use it for the overflow check here.
        _snap_safe_bottom = pdf.h - 18
        pdf.set_auto_page_break(False)
        for ri, row in enumerate(snap_rows):
            row_y = pdf.get_y()
            # Stop rendering rows that would overflow into the footer area.
            if row_y + row_h_s > _snap_safe_bottom:
                break
            if ri % 2 == 0:
                pdf.set_fill_color(243, 244, 236)
            else:
                pdf.set_fill_color(255, 255, 255)
            pdf.set_text_color(30, 41, 59)
            pdf.set_font(font_name, "", 8)
            for j, sc in enumerate(snap_cols):
                cell_val = _fmt_snap(str(row.get(sc, "")), sc)
                pdf.set_xy(pdf.l_margin + j * col_w_s, row_y)
                pdf.cell(col_w_s, row_h_s, _safe(cell_val), fill=True)
            pdf.set_y(row_y + row_h_s)
        pdf.set_auto_page_break(True, margin=18)

        pdf.set_text_color(0, 0, 0)
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
        chart_type=turn.get("chart_type", ""),
        lang=_detect_language(turn["question"]),
    )


def _branded_table_html(df: pd.DataFrame) -> str:
    """Render a DataFrame as a branded HTML table matching the EDP olive theme.

    Used instead of st.dataframe + pandas Styler because Styler CSS (especially
    thead background) is not reliably applied across Streamlit versions.
    """
    th_style = (
        "background:#4B5320;color:white;padding:8px 12px;"
        "font-size:12px;font-weight:600;letter-spacing:0.04em;"
        "border-bottom:2px solid #3A4118;text-align:left;white-space:nowrap;"
    )
    td_style_base = "padding:8px 12px;font-size:13px;color:#1e293b;border-bottom:1px solid #e2e8f0;"
    row_bg = ("#F3F4EC", "#ffffff")

    headers = "".join(
        f'<th style="{th_style}">{html_lib.escape(str(col))}</th>' for col in df.columns
    )
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = row_bg[i % 2]
        cells = "".join(
            f'<td style="{td_style_base}background:{bg};">{html_lib.escape(str(v))}</td>'
            for v in row
        )
        rows_html += f"<tr>{cells}</tr>"

    return (
        '<div style="overflow-x:auto;border-radius:8px;border:1px solid #e2e8f0;">'
        f'<table style="width:100%;border-collapse:collapse;">'
        f"<thead><tr>{headers}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>"
    )


def _render_turn(turn: dict, form_key: str) -> None:
    """Render one Q&A turn's answer content. Called inside a turn card."""
    lang = _detect_language(turn["question"])
    is_analytical = bool(turn.get("sql"))

    # Insight card — full text, no clamping.
    escaped = html_lib.escape(turn["insight"]).replace("\n", "<br>")
    st.markdown(
        f'<div class="insight-card">{escaped}</div>',
        unsafe_allow_html=True,
    )

    # KPI tiles — single flex row so all cards stretch to equal height automatically.
    # Using st.columns would put each card in a separate Streamlit container,
    # preventing cross-card height equalisation. One st.markdown with a flex
    # wrapper gives display:flex align-items:stretch for free.
    kpi_tiles = _extract_kpi_tiles(turn.get("columns", []), turn.get("rows", []), lang)
    if kpi_tiles:
        n_tiles = len(kpi_tiles)
        # Single tile: cap width to ~33% so it doesn't span the full card width.
        wrapper_style = (
            'display:flex;gap:12px;margin:12px 0 4px 0;align-items:stretch;'
            + ('max-width:34%;' if n_tiles == 1 else '')
        )
        tiles_html = f'<div style="{wrapper_style}">'
        for metric_lbl, value, sub_lbl, badge in kpi_tiles:
            badge_color = "#16a34a" if badge.startswith("+") else "#dc2626"
            badge_html = (
                f'<div style="color:{badge_color};font-size:12px;font-weight:600;'
                f'margin-top:4px">{badge}</div>'
                if badge
                else ""
            )
            tiles_html += (
                f'<div style="flex:1;background:#f0f7ff;border-radius:8px;'
                f'border-top:3px solid #4B5320;padding:12px 16px 10px 16px;'
                f'display:flex;flex-direction:column;">'
                f'<div style="font-size:11px;color:#4B5320;font-weight:600;'
                f'letter-spacing:0.05em;margin-bottom:4px">{metric_lbl.upper()}</div>'
                f'<div style="font-size:22px;font-weight:700;color:#0f172a;'
                f'line-height:1.1">{value}</div>'
                f'<div style="font-size:12px;color:#64748b;margin-top:3px">{sub_lbl}</div>'
                f'{badge_html}</div>'
            )
        tiles_html += '</div>'
        st.markdown(tiles_html, unsafe_allow_html=True)

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
                st.markdown(_branded_table_html(df), unsafe_allow_html=True)
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
