select
    categoryid as category_id,
    parentid as parent_category_id
from {{ source('raw', 'category_tree') }}