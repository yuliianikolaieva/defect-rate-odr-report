# Stores Defect Rate / ODR · Bolt Food UA

Тижневий аналіз якості виконання замовлень у Stores: складові Order Defect Rate, топ партнери,
проблеми та дії, порівняння України з ТОП-3 країнами Bolt за обсягом Stores.

**Живий звіт (UA):** https://yuliianikolaieva.github.io/defect-rate-odr-report/
**Live report (EN, side navigation):** https://yuliianikolaieva.github.io/defect-rate-odr-report/en/

Оновлення: щопонеділка о **10:00 за Києвом** (GitHub Actions → Databricks → `data.js`).

## Ключові цифри (вікно 29 черв — 14 вер 2026)
| Метрика | Значення |
|---|---|
| UA Stores ODR (сер. 12 тижнів) | 20.1% |
| ODR останній тиждень (14 вер) | 20.4% |
| Order replacement rate | 20.7% |
| Частка quantity у дефектах | 66.2% |
| VARUS ODR | 57.9% (останній тиждень 66.0%) |

## Що всередині
- Складові ODR: quantity / weight / price + окремо replacement
- Тижневий тренд ринку UA та по 9 ключових брендах (12 повних тижнів)
- Окремий ODR MWB без VARUS (LOKO + Рукавичка, зважено) + графік
- Таблиця ключових партнерів (item-level метрики + ODR + Δ за період + статус)
- Інтерактивний drill-down проблемних партнерів: категорії → SKU → quantity/OOS-сигнал, replacement, weight і price
- ТОП-15 партнерів UA Stores за обсягом замовлень (єдине визначення ODR)
- Проблема + конкретна дія по кожному партнеру
- Деталізація: найгірші магазини VARUS, категорії з найбільшим вкладом, гарячі SKU
- Порівняння: Румунія / Естонія / Литва / Україна + ТОП-5 партнерів кожної країни
- Country-level Order Replacement Rate UA з розкладом на ENT / MM / SMB
- План P0 / P1 та цілі
- Глосарій: визначення кожної метрики і як її покращити

## Порівняння країн (29 черв — 14 вер 2026, однакове визначення ODR)
| Країна | ODR ринку | Сер. ODR топ-5 | Без 1P |
|---|---|---|---|
| Румунія | 38.7% | 40.7% | 50.8% |
| Естонія | 14.1% | 14.1% | 17.5% |
| Литва | 24.6% | 20.1% | 25.1% |
| **Україна** | **20.1%** | **20.1%** | **20.1%** |

В Україні немає 1P (Bolt Market), тому коректний бенчмарк — колонка «без 1P».

## Дані
- **Databricks**: `main.ng_delivery.dim_basket_item_delivery` × `dim_provider_v2`
- Вікно: 12 повних тижнів до поточного (оновлення щопонеділка 10:00 Київ)
- Фільтри: `order_state = 'delivered'`, `basket_item_is_dish = true`, `delivery_vertical LIKE 'store_%'`

## Обмеження
- «Середній ODR топ-5» — просте середнє, не зважене на обсяг
- Quantity item-rate: чисельник по всіх dish items з eater-impact, знаменник — active items
- Обсяги weight/price на stacked-графіку використовують ширші adjustment flags, тож стрибки якості даних видно окремо від ODR
