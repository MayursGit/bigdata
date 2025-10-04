select 
  c.client_id as UserId,
  u.name as UserName,
  c.id as CardId,
  c.card_number as CardNumber,
  c.card_type as CardType,
  c.card_brand as CardBrand,
  c.expires as ExpireeDate,
  c.cvv as CardCVV,
  c.num_cards_issued as NumCards,
  c.credit_limit as CreditLimit,
  c.acct_open_date as OpenDate,
  "" as CloseDate,
  c.year_pin_last_changed as PinChangeYear,
  case when c.has_chip='YES' then 'Y' when c.has_chip='NO' then 'N' else null end as ChipInd,
  case when c.card_on_dark_web='YES' then 'Y' else 'N' end as DarkWebInd,
  c.batch_id as BatchId,
  c.create_date as CreateDate,
  c.create_user as CreateUser,
  "CreditCardExtract" as CreateProcess,
  c.update_date as UpdateDate,
  c.update_user as UpdateUser,
  "" as UpdateProcess
  from cards c join users u
  on c.client_id = u.id
  where upper(c.card_type) like '%CREDIT%';