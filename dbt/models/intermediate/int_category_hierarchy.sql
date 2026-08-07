with recursive category_hierarchy as (
    -- Root categories
    select
        category_id,
        parent_category_id,

        category_id as root_category_id,
        1 as category_depth,
        category_id::text as category_path
    from {{ ref('stg_category_tree') }}
    where parent_category_id is null

    union all

    -- Recursive step: attach children to categories already found
    select
        child.category_id,
        child.parent_category_id,

        parent.root_category_id,
        parent.category_depth + 1 as category_depth,

        parent.category_path
            || ' > '
            || child.category_id::text as category_path
    from {{ ref('stg_category_tree') }} as child
    inner join category_hierarchy as parent
        on child.parent_category_id = parent.category_id
)

select
    category_id,
    parent_category_id,
    root_category_id,
    category_depth,
    category_path
from category_hierarchy