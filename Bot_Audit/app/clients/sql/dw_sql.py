# ===============================
# PLAYERS
# ===============================

PLAYERS_BY_HOUR = """
SELECT 
    DATEADD(HOUR, DATEDIFF(HOUR, 0, Registration_Datetime), 0) AS hora,
    ISNULL(COUNT(DISTINCT User_Id), 0) AS qtde_registros_dw
FROM players WITH (NOLOCK)
WHERE Registration_Datetime >= CAST(? AS DATETIME)
  AND Registration_Datetime < DATEADD(DAY, 1, CAST(? AS DATETIME))
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, Registration_Datetime), 0)
ORDER BY hora
"""


# ===============================
# FTD
# ===============================

FTD_BY_HOUR = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, FTD_Datetime), 0) AS hora,
    COUNT(DISTINCT User_Id) AS ftd_users_dw
FROM f_t_d_ WITH (NOLOCK)
WHERE FTD_Datetime >= ?
  AND FTD_Datetime < DATEADD(DAY, 1, ?)
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, FTD_Datetime), 0)
ORDER BY hora
"""


# ===============================
# PAYMENTS
# ===============================

PAYMENTS_BY_HOUR = """
  SELECT
      DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0) AS hora,
      SUM(Deposits_Amount) AS deposit -- Chamamos de 'deposit' para o wrapper renomear
  FROM payments WITH (NOLOCK)
  WHERE Date_time >= ?
    AND Date_time < DATEADD(DAY, 1, ?)
    AND Status = 'Completed'
  GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0)
  ORDER BY hora
"""


# ===============================
# WALLET - CASINO
# ===============================

WALLET_BY_HOUR = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0) AS hora,
    SUM(Bet_Count) AS Apostas,
    SUM(Turnover)  AS Turnover,
    SUM(GGR)       AS GGR,
    SUM(NGR)       AS NGR
FROM Games (NOLOCK)
WHERE Date_time >= ?
  AND Date_time < DATEADD(DAY, 1, ?)
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0)
ORDER BY hora
"""

# ===============================
# WALLET - SPORTS
# ===============================

WALLET_BY_HOUR_SPORTS = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0) AS hora,
    SUM(Bet_Count) AS Apostas,
    SUM(Turnover)  AS Turnover,
    SUM(GGR)       AS GGR,
    SUM(NGR)       AS NGR
FROM Games_sports WITH (NOLOCK)
WHERE Date_time >= ?
  AND Date_time < DATEADD(DAY, 1, ?)
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, Date_time), 0)
ORDER BY hora
"""