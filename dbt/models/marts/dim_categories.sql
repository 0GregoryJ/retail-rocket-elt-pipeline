select
    category_id,
    parent_category_id,
    root_category_id,
    category_depth,
    category_path,

    parent_category_id is null as is_root_category,

    'Category ' || category_id::text as category_label,
    'Root Category ' || root_category_id::text as root_category_label

from {{ ref('int_category_hierarchy') }}