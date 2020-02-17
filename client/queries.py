def get_data(request):
    if request == 'coat_variations':

        query = """\
                    select 
                        animal_info.coat, 
                        animal_info.colors_primary, 
                        count(*) as num 
                    from animal_info 
                    where animal_info.coat is not NULL 
                        and animal_info.colors_primary is not NULL 
                    group by animal_info.coat, animal_info.colors_primary;"""

        return query

    if request == 'shelter_med':
        query = """
                select 
                    organization_name,
                    num_declawed,
                    num_not_house_trained,
                    num_shots_not_current,
                    num_special_needs,
                    num_not_fixed
                from
                    (
                        select
                            declaw.organization_id,
                            num_declawed,
                            num_not_house_trained,
                            num_shots_not_current,
                            num_special_needs,
                            num_not_fixed
                        from
                            (select
                                organization_id,
                                count(declawed) as num_declawed
                            from animal_medical_info
                            left join animal_info
                                on animal_info.id = animal_medical_info.id
                            where declawed is True
                            group by organization_id) declaw

                        left join
                            (select
                                organization_id,
                                count(house_trained) as num_not_house_trained
                            from animal_medical_info
                            left join animal_info
                                on animal_info.id = animal_medical_info.id
                            where house_trained is False
                            group by organization_id) house
                        on declaw.organization_id = house.organization_id
                        left join
                            (select
                                organization_id,
                                count(shots_current) as num_shots_not_current
                            from animal_medical_info
                            left join animal_info
                                on animal_info.id = animal_medical_info.id
                            where shots_current is False
                            group by organization_id) shots
                        on declaw.organization_id = shots.organization_id
                        left join
                            (select
                                organization_id,
                                count(special_needs) as num_special_needs
                            from animal_medical_info
                            left join animal_info
                                on animal_info.id = animal_medical_info.id
                            where special_needs is True
                            group by organization_id) special
                        on declaw.organization_id = special.organization_id
                        left join
                            (select
                                organization_id,
                                count(spayed_neutered) as num_not_fixed
                            from animal_medical_info
                            left join animal_info
                                on animal_info.id = animal_medical_info.id
                            where spayed_neutered is False
                            group by organization_id) fixed
                        on declaw.organization_id = fixed.organization_id
                    ) main
                left join organization_info
                on main.organization_id = organization_info.organization_id
                ;
            """

        return query
    if request == 'null_temp':
        query = """
                select
                    *,
                    (
                        select
                            count(*) as total_rows
                        from animal_temperment
                    )
                from
                    (
                        (select 'dogs' as column_name,  count(*) as num_null from animal_temperment where dogs is null)
                        union
                        (select 'id' as column_name,  count(*) as num_null from animal_temperment where id is null )
                        union
                        (select 'children' as column_name,  count(*) as num_null from animal_temperment where children is null )
                        union
                        (select 'cats' as column_name,  count(*) as num_null from animal_temperment where cats is null )
                    ) nulls;"""
        return query
    if request == 'null':
        query = """
                    (
                         select
                         'animal_temperment' as table_name,
                            *,
                            (
                                select
                                    count(*) as total_rows
                                from animal_temperment
                            )
                        from
                            (
                                (select 'dogs' as column_name,  count(*) as num_null from animal_temperment where dogs is null)
                                union
                                (select 'id' as column_name,  count(*) as num_null from animal_temperment where id is null )
                                union
                                (select 'children' as column_name,  count(*) as num_null from animal_temperment where children is null )
                                union
                                (select 'cats' as column_name,  count(*) as num_null from animal_temperment where cats is null )
                            ) nulls
                    )
                    union
                    (
                         select
                                 'animal_medical_info' as table_name,
                            *,
                            (
                                select
                                    count(*) as total_rows
                                from animal_medical_info
                            )
                        from
                            (
                                (select 'declawed' as column_name,  count(*) as num_null from animal_medical_info where declawed is null)
                                union
                                (select 'id' as column_name,  count(*) as num_null from animal_medical_info where id is null )
                                union
                                (select 'house_trained' as column_name,  count(*) as num_null from animal_medical_info where house_trained is null )
                                union
                                (select 'shots_current' as column_name,  count(*) as num_null from animal_medical_info where shots_current is null )
                                union
                                (select 'special_needs' as column_name,  count(*) as num_null from animal_medical_info where special_needs is null )
                                union
                                (select 'spayed_neutered' as column_name,  count(*) as num_null from animal_medical_info where spayed_neutered is null )
                            ) nulls
                    )
                    union
                    (
                        select
                             'animal_info' as table_name,
                            *,
                            (
                                select
                                    count(*) as total_rows
                                from animal_info
                            )
                        from
                            (
                                (select 'organization_id' as column_name,  count(*) as num_null from animal_info where organization_id is null)
                                union
                                (select 'id' as column_name,  count(*) as num_null from animal_info where id is null )
                                union
                                (select 'name' as column_name,  count(*) as num_null from animal_info where animal_info.name is null )
                                union
                                (select 'size' as column_name,  count(*) as num_null from animal_info where animal_info.size is null )
                                union
                                (select 'age' as column_name,  count(*) as num_null from animal_info where age is null )
                                union
                                (select 'gender' as column_name,  count(*) as num_null from animal_info where gender is null )
                                union
                                (select 'breeds_primary' as column_name,  count(*) as num_null from animal_info where breeds_primary is null )
                                union
                                (select 'breeds_secondary' as column_name,  count(*) as num_null from animal_info where breeds_secondary is null )
                                union
                                (select 'breeds_mixed' as column_name,  count(*) as num_null from animal_info where breeds_mixed is null )
                                union
                                (select 'breeds_unknown' as column_name,  count(*) as num_null from animal_info where breeds_unknown is null )
                                union
                                (select 'colors_primary' as column_name,  count(*) as num_null from animal_info where colors_primary is null )
                                union
                                (select 'colors_secondary' as column_name,  count(*) as num_null from animal_info where colors_secondary is null )
                                union
                                (select 'colors_mixed' as column_name,  count(*) as num_null from animal_info where colors_mixed is null )
                                union
                                (select 'coat' as column_name,  count(*) as num_null from animal_info where coat is null )
                                union
                                (select 'published_at' as column_name,  count(*) as num_null from animal_info where published_at is null )
                            ) nulls
                    )
                    union
                    (
                         select
                            'animal_description' as table_name,
                            *,
                            (
                                select
                                    count(*) as total_rows
                                from animal_description
                            )
                        from
                            (
                                (select 'description' as column_name,  count(*) as num_null from animal_description where description is null)
                                union
                                (select 'id' as column_name,  count(*) as num_null from animal_description where id is null )
                            ) nulls
                    )
                    union
                    (
                        select
                                'animal_status' as table_name,
                            *,
                            (
                                select
                                    count(*) as total_rows
                                from animal_status
                            )
                        from
                            (
                                (select 'status' as column_name,  count(*) as num_null from animal_status where status is null)
                                union
                                (select 'id' as column_name,  count(*) as num_null from animal_status where id is null )
                                union
                                (select 'status_changed_at' as column_name,  count(*) as num_null from animal_status where status_changed_at is null )
                            ) nulls
                    );
                    """
        return query


    else:
        raise Exception('Your request is not currently supported')

