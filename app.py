def poll_feedbacks():
    """
    Получаем новые отзывы через API WB
    Документация: https://openapi.wildberries.ru/feedbacks
    """
    if not WB_TOKEN:
        tg_send("⚠️ WB_TOKEN не задан — опрос отзывов не запущен")
        return

    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    headers = {"Authorization": WB_TOKEN}

    while True:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json()
                feedbacks = data.get("data", {}).get("feedbacks", [])

                for fb in feedbacks:
                    fb_id = fb.get("id")
                    key = f"FB:{fb_id}"
                    if key in seen:
                        continue
                    seen.add(key)

                    rating = fb.get("productValuation", "—")
                    text = (fb.get("text") or "").strip()
                    nm_id = fb.get("nmId") or "—"

                    tg_send(
                        "📝 Новый отзыв WB\n"
                        f"Товар (nmId): {nm_id}\n"
                        f"Оценка: {rating}\n"
                        f"Текст: {text[:900]}"
                    )
        except Exception as e:
            tg_send(f"⚠️ Ошибка опроса отзывов: {e}")

        time.sleep(POLL_SECONDS)
