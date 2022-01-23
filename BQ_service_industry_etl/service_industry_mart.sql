create or replace table `data-engineer-5125-336206.workflow_test.category_add_service_industry_sales` as( 
with middle_category_add_tbl as(
select
  date
  ,item
  ,case when industry in('その他') then 'その他'
  else regexp_extract(industry,'^..(.*)') 
  end as middle_category_name
  ,area
  ,cast(case when regexp_extract(industry,'^\\d{1,2}') = '4' then '46'
  else regexp_extract(industry,'^\\d{1,2}')
  end as int)  as middle_category  
  ,value
  ,unit
from 
  `data-engineer-5125-336206.workflow_test.SI_raw_aferTransaction`
where 
  industry not like '82aうち社会教育，職業・教育支援施設'
  and industry not like '82bうち学習塾，教養・技能教授業'
order by 3
)
/*
37~41は大カテゴリGに分類
42~49は大カテゴリHに分類
68~70は大カテゴリKに分類
72~74は大カテゴリLに分類
75~77は大カテゴリMに分類
78~80は大カテゴリNに分類
*/
,large_category_add_tbl as(
select 
  date
  ,item
  ,middle_category_name
  ,area
  ,case
  when middle_category between 37 and 41 then 'G'
  when middle_category between 42 and 49 then 'H'
  when middle_category between 68 and 70 then 'K'
  when middle_category between 72 and 74 then 'L'
  when middle_category between 75 and 77 then 'M'
  when middle_category between 78 and 80 then 'N'
  when middle_category = 82 then 'O'
  when middle_category between 83 and 85 then 'P'
  when middle_category between 88 and 95 then 'R'
  else null
  end as large_category
  ,middle_category
  ,value
  ,unit
from
  middle_category_add_tbl
where 
  middle_category is not null
  or 
  middle_category_name in ('その他')
)

select 
  date
  ,item
  ,area
  ,middle_category_name
  ,large_category
  ,middle_category
  ,value
  ,unit
from
  large_category_add_tbl
order by middle_category asc
)

create or replace table `data-engineer-5125-336206.workflow_test.large_category_table` as(
select 
  normalize(regexp_extract(industry,'.'),NFKC) as large_category
 ,regexp_extract(industry,'^.(.*)') as large_category_name
from 
  `data-engineer-5125-336206.workflow_test.SI_raw_aferTransaction`
where 
 regexp_extract(industry,'^\\d{1,2}') is null
 and 
 industry not in ('合計','サービス産業計','その他')
group by 1,2
)

create or replace table `data-engineer-5125-336206.workflow_test.service_industry_mart` as
select 
  date
  ,item
  ,area
  ,category_add_service_industry_sales.large_category
  ,large_category_name
  ,middle_category
  ,middle_category_name
  ,value
  ,unit
from 
 `data-engineer-5125-336206.workflow_test.category_add_service_industry_sales` as category_add_service_industry_sales
left join `data-engineer-5125-336206.workflow_test.large_category_table` as large_category_tbl
  on category_add_service_industry_sales.large_category = large_category_tbl.large_category
order by 6,1