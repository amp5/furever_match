
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


    else:
        raise Exception('Your request is not currently supported')


