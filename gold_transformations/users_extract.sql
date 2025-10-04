select
    current_date() as StartDate,
    cast(Null as date) as EndDate,
    u.id as UserId,
    u.name as UserName,
    u.gender as Gender,
    u.birth_year as BirthYear,
    u.birth_month as BirthMonth,
    u.current_age as CurrentAge,
    u.retirement_age as RetirementAge,
    u.address as UserAddress1,
    u.latitude as UserAddress2,
    u.longitude as UserAddress3,
    u.per_capita_income as PerCapitaIncome,
    u.yearly_income as UserIncome,
    u.total_debt as UserDebt,
    u.credit_score as CreditScore,
    agg.total_credit_limit as TotalCreditLimit,
    u.num_credit_cards as NumOfCards,
    u.batch_id as BatchId,
    "A" as ActiveStatus,
    u.create_date as CreateDate,
    u.create_user as CreateUser,
    "UsersExtract" as CreateProcess,
    u.update_date as UpdateDate,
    u.update_user as UpdateUser,
    "" as UpdateProcess
    from users u left join (
        select c.client_id, sum(cast(replace(c.credit_limit,'$','') as double)) as total_credit_limit
            from cards c
            group by c.client_id ) agg
    on u.id=agg.client_id;