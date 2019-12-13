require 'rails_helper'
describe NaaccrEtl do
  before(:each) do
    NaaccrEtl::SpecSetup.teardown
    @person_1 = FactoryBot.create(:person)
    @person_2 = FactoryBot.create(:person)
    @legacy = false
  end

  after(:each) do
    # NaaccrEtl::SpecSetup.teardown
  end

  describe "For an 'ICDO Condition' that maps to itself" do
    before(:each) do
      @diagnosis_date = '19981022'
      # @diagnosis_date = '20170630'
      @histology_site = '8140/3-C61.9'
      #390=Date of Diagnosis
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: '390' \
        , naaccr_item_value:  @diagnosis_date \
        , histology: '8140/3' \
        , site: 'C61.9' \
        , histology_site:  @histology_site \
      )
      @condition_concept = NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
      NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
    end

    it "Creates an entry in the CONDITION_OCCURRENCE table", focus: false do
      expect(ConditionOccurrence.count).to eq(1)
      condition_occurrence = ConditionOccurrence.first
      expect(condition_occurrence.condition_concept_id).to eq(@condition_concept.concept_id)
      expect(condition_occurrence.person_id).to eq(@person_1.person_id)
      expect(condition_occurrence.condition_start_date).to eq(Date.parse(@diagnosis_date))
      expect(condition_occurrence.condition_start_datetime).to eq(Date.parse(@diagnosis_date))
      expect(condition_occurrence.condition_type_concept_id).to eq(32534) #32534=‘Tumor registry’ type concept
      expect(condition_occurrence.condition_source_value).to eq(@histology_site)
      expect(condition_occurrence.condition_source_concept_id).to eq(@condition_concept.concept_id)
    end

    it "Creates an entry in the EPISODE table", focus: false do
      expect(Episode.count).to eq(1)
      episode = Episode.first
      expect(episode.person_id).to eq(@person_1.person_id)
      expect(episode.episode_concept_id).to eq(32528) #32528='Disease First Occurrence'
      expect(episode.episode_start_datetime).to eq(Date.parse(@diagnosis_date))
      expect(episode.episode_end_datetime).to be_nil
      expect(episode.episode_object_concept_id).to eq(@condition_concept.concept_id)
      expect(episode.episode_type_concept_id).to eq(32546)
      expect(episode.episode_source_value).to eq(@histology_site)
      expect(episode.episode_source_concept_id).to eq(@condition_concept.concept_id)
    end
  end

  describe "For an 'ICDO Condition' that maps to a SNOMED concept" do
    before(:each) do
      @diagnosis_date = '20170630'
      @histology_site = '8560/3-C54.1'
      #390=Date of Diagnosis
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: '390' \
        , naaccr_item_value:  @diagnosis_date \
        , histology: '8560/3' \
        , site: 'C54.1' \
        , histology_site:  @histology_site \
      )
      @condition_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
      @condition_source_concept = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
      NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
    end

    it "Creates an entry in the CONDITION_OCCURRENCE table", focus: false do
      expect(ConditionOccurrence.count).to eq(1)
      condition_occurrence = ConditionOccurrence.first
      expect(condition_occurrence.condition_concept_id).to eq(@condition_concept.concept_id)
      expect(condition_occurrence.person_id).to eq(@person_1.person_id)
      expect(condition_occurrence.condition_start_date).to eq(Date.parse(@diagnosis_date))
      expect(condition_occurrence.condition_start_datetime).to eq(Date.parse(@diagnosis_date))
      expect(condition_occurrence.condition_type_concept_id).to eq(32534) #32534=‘Tumor registry’ type concept
      expect(condition_occurrence.condition_source_value).to eq(@histology_site)
      expect(condition_occurrence.condition_source_concept_id).to eq(@condition_source_concept.concept_id)
    end

    it "Creates an entry in the EPISODE table ", focus: false do
      expect(Episode.count).to eq(1)
      episode = Episode.first
      expect(episode.person_id).to eq(@person_1.person_id)
      expect(episode.episode_concept_id).to eq(32528) #32528='Disease First Occurrence'
      expect(episode.episode_start_datetime).to eq(Date.parse(@diagnosis_date))
      expect(episode.episode_end_datetime).to be_nil
      expect(episode.episode_object_concept_id).to eq(@condition_concept.concept_id)
      expect(episode.episode_type_concept_id).to eq(32546)
      expect(episode.episode_source_value).to eq(@histology_site)
      expect(episode.episode_source_concept_id).to eq(@condition_source_concept.concept_id)
    end
  end

  describe 'Creating entries in MEASUREMENT table for a standard categorical schema-independent diagnosis modifier' do
    before(:each) do
      @diagnosis_date = '20170630'
      @histology_site = '8140/3-C61.9'
      @naaccr_item_number = '1182'          #Lymph-vascular Invasion
      @naaccr_item_value = '1'              #Lymph-vascular Invasion Present/Identified

      #390=Date of Diagnosis.
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: '390' \
        , naaccr_item_value: @diagnosis_date \
        , histology: '8140/3' \
        , site: 'C61.9' \
        , histology_site: @histology_site \
      )

      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: @naaccr_item_number \
        , naaccr_item_value: @naaccr_item_value  \
        , histology: '8140/3' \
        , site: 'C61.9' \
        , histology_site: @histology_site \
      )
      @measurement_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      @measurement_source_concept = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      @measurement_value_as_concept = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_item_number}@#{@naaccr_item_value}")
      @condition_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
      NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
    end

    it 'pointing to CONDITION_OCCURRENCE' do
      expect(Measurement.where(modifier_of_field_concept_id: 1147127).count).to eq(1)       #1147127 = 'condition_occurrence.condition_occurrence_id'
      measurement = Measurement.where(modifier_of_field_concept_id: 1147127).first
      expect(measurement.person_id).to eq(@person_1.person_id)
      expect(measurement.measurement_concept_id).to eq(@measurement_concept.concept_id)
      expect(measurement.measurement_date).to eq(Date.parse(@diagnosis_date))
      expect(measurement.measurement_time).to be_nil
      expect(measurement.measurement_datetime).to eq(Date.parse(@diagnosis_date))
      expect(measurement.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement.value_as_concept_id).to eq(@measurement_value_as_concept.concept_id)
      expect(measurement.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement.value_source_value).to eq(@naaccr_item_value)
      expect(ConditionOccurrence.count).to eq(1)
      condition_occurrence = ConditionOccurrence.first
      expect(measurement.modifier_of_event_id).to eq(condition_occurrence.condition_occurrence_id)
      expect(measurement.modifier_of_field_concept_id).to eq(1147127) #‘condition_occurrence.condition_occurrence_id’ concept
    end

    it 'pointing to EPISODE' do
      expect(Measurement.where(modifier_of_field_concept_id: 1000000003).count).to eq(1)
      measurement = Measurement.where(modifier_of_field_concept_id: 1000000003).first
      expect(measurement.person_id).to eq(@person_1.person_id)
      expect(measurement.measurement_concept_id).to eq(@measurement_concept.concept_id)
      expect(measurement.measurement_date).to eq(Date.parse(@diagnosis_date))
      expect(measurement.measurement_time).to be_nil
      expect(measurement.measurement_datetime).to eq(Date.parse(@diagnosis_date))
      expect(measurement.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement.value_as_concept_id).to eq(@measurement_value_as_concept.concept_id)
      expect(measurement.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement.value_source_value).to eq(@naaccr_item_value)
      expect(Episode.count).to eq(1)
      episode = Episode.first
      expect(measurement.modifier_of_event_id).to eq(episode.episode_id)
      expect(measurement.modifier_of_field_concept_id).to eq(1000000003) #‘‘episode.episode_id’ concept
    end
  end

  describe 'Creating entries in MEASUREMENT table for a standard categorical schema-dependent NAACCR variable diagnosis modifier' do
    before(:each) do
      @diagnosis_date_1 = '20170630'
      @histology_1 = '9421/3'
      @site_1 = 'C71.3'
      @histology_site_1 = "#{@histology_1}-#{@site_1}"

      @diagnosis_date_2 = '20180630'
      @histology_2 = '8507/3'
      @site_2 = 'C50.8'
      @histology_site_2 = "#{@histology_2}-#{@site_2}"

      @naaccr_item_number = '2880'          #?

      @naaccr_schema_concept_code_1 = 'brain'
      @naaccr_item_value_1 = '020'            #"Grade II"

      @naaccr_schema_concept_code_2 = 'breast'
      @naaccr_item_value_2 = '020'            #"Negative/normal; within normal limits"

      #390=Date of Diagnosis.
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: '390' \
        , naaccr_item_value: @diagnosis_date_1 \
        , histology: @histology_1  \
        , site: @site_1 \
        , histology_site: @histology_site_1 \
      )

      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: @naaccr_item_number \
        , naaccr_item_value: @naaccr_item_value_1  \
        , histology: @histology_1 \
        , site: @site_1 \
        , histology_site: @histology_site_1 \
      )

      #390=Date of Diagnosis.
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_2.person_id \
        , record_id: '2' \
        , naaccr_item_number: '390' \
        , naaccr_item_value: @diagnosis_date_2 \
        , histology: @histology_2  \
        , site: @site_2 \
        , histology_site: @histology_site_2 \
      )

      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_2.person_id \
        , record_id: '2' \
        , naaccr_item_number: @naaccr_item_number \
        , naaccr_item_value: @naaccr_item_value_2  \
        , histology: @histology_2 \
        , site: @site_2 \
        , histology_site: @histology_site_2 \
      )

      @measurement_concept_1 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: "#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}")
      @measurement_source_concept_1 = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      concept_code = "#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}@#{@naaccr_item_value_1}"
      @measurement_value_as_concept_1 = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}@#{@naaccr_item_value_1}")
      @condition_concept_1 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site_1)

      @measurement_concept_2 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: "#{@naaccr_schema_concept_code_2}@#{@naaccr_item_number}")
      @measurement_source_concept_2 = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      @measurement_value_as_concept_2 = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_schema_concept_code_2}@#{@naaccr_item_number}@#{@naaccr_item_value_2}")
      @condition_concept_2 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site_2)
      NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
    end

    it 'pointing to CONDITION_OCCURRENCE', focus: false do
      expect(ConditionOccurrence.count).to eq(2)
      expect(Measurement.where(modifier_of_field_concept_id: 1147127).count).to eq(2) #1147127 = 'condition_occurrence.condition_occurrence_id'

      condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept_1.concept_id).first
      #1147127 = 'condition_occurrence.condition_occurrence_id'

      measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id, measurement_concept_id: @measurement_concept_1.concept_id, value_as_concept_id: @measurement_value_as_concept_1.concept_id).first
      expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_time).to be_nil
      expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_1.measurement_source_value).to eq("#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}")
      expect(measurement_1.measurement_source_concept_id).to eq(@measurement_concept_1.concept_id)
      expect(measurement_1.value_source_value).to eq(@naaccr_item_value_1)

      condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept_2.concept_id).first
      measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept_2.concept_id, value_as_concept_id: @measurement_value_as_concept_2.concept_id).first
      expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_time).to be_nil
      expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_2.measurement_source_value).to eq("#{@naaccr_schema_concept_code_2}@#{@naaccr_item_number}")
      expect(measurement_2.measurement_source_concept_id).to eq(@measurement_concept_2.concept_id)
      expect(measurement_2.value_source_value).to eq(@naaccr_item_value_2)
    end

    it 'pointing to EPISODE', focus: false do
      expect(ConditionOccurrence.count).to eq(2)
      expect(Measurement.where(modifier_of_field_concept_id: 1000000003).count).to eq(2) #1000000003 = ‘episode.episode_id’ concept

      condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept_1.concept_id).first
      #1000000003 = ‘episode.episode_id’ concept
      measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_concept_1.concept_id, value_as_concept_id: @measurement_value_as_concept_1.concept_id).first
      expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_time).to be_nil
      expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_1.measurement_source_value).to eq("#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}")
      expect(measurement_1.measurement_source_concept_id).to eq(@measurement_concept_1.concept_id)
      expect(measurement_1.value_source_value).to eq(@naaccr_item_value_1)

      condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept_2.concept_id).first
      measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept_2.concept_id, value_as_concept_id: @measurement_value_as_concept_2.concept_id).first
      expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_time).to be_nil
      expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_2.measurement_source_value).to eq("#{@naaccr_schema_concept_code_2}@#{@naaccr_item_number}")
      expect(measurement_2.measurement_source_concept_id).to eq(@measurement_concept_2.concept_id)
      expect(measurement_2.value_source_value).to eq(@naaccr_item_value_2)
    end
  end

  describe 'Creating entries in MEASUREMENT table for a standard categorical schema-dependent NAACCR value diagnosis modifier' do
    before(:each) do
      # @histology = '8070/2'
      # @site = 'C00.3'
      # @histology_site = "#{@histology}-#{@site}"
      # @diagnosis_date_1 = '20170630'
      # @diagnosis_date_2 = '20180630'
      #
      # @naaccr_item_number = '772'               #EOD Primary Tumor
      # @naaccr_item_value = '200'                #200
      # @naaccr_schema_concept_code = 'lip_upper'

      @histology = '9180/3'
      @site = 'C67.7'
      @histology_site = "#{@histology}-#{@site}"
      @diagnosis_date_1 = '20170630'
      @diagnosis_date_2 = '20180630'

      @naaccr_item_number = '772'               #EOD Primary Tumor
      @naaccr_item_value = '200'                #200
      @naaccr_schema_concept_code = 'bladder'

      #Person 1
      #390=Date of Diagnosis
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: '390' \
        , naaccr_item_value: @diagnosis_date_1 \
        , histology: @histology \
        , site: @site \
        , histology_site: @histology_site \
      )

      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_1.person_id \
        , record_id: '1' \
        , naaccr_item_number: @naaccr_item_number \
        , naaccr_item_value: @naaccr_item_value  \
        , histology: @histology \
        , site: @site \
        , histology_site: @histology_site \
      )

      #Person 2
      #390=Date of Diagnosis
      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_2.person_id \
        , record_id: '2' \
        , naaccr_item_number: '390' \
        , naaccr_item_value: @diagnosis_date_2 \
        , histology: @histology \
        , site: @site \
        , histology_site: @histology_site \
      )

      FactoryBot.create(:naaccr_data_point \
        , person_id: @person_2.person_id \
        , record_id: '2' \
        , naaccr_item_number: @naaccr_item_number \
        , naaccr_item_value: @naaccr_item_value  \
        , histology: @histology \
        , site: @site \
        , histology_site: @histology_site \
      )

      @measurement_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      @measurement_source_concept = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
      @measurement_value_as_concept = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_schema_concept_code}@#{@naaccr_item_number}@#{@naaccr_item_value}")

      @condition_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
      NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
    end

    it 'pointing to CONDITION_OCCURRENCE', focus: false do
      expect(ConditionOccurrence.count).to eq(2)
      expect(Measurement.where(modifier_of_field_concept_id: 1147127).count).to eq(2) #1147127 = 'condition_occurrence.condition_occurrence_id'

      condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept.concept_id).first
      #1147127 = 'condition_occurrence.condition_occurrence_id'
      measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept.concept_id).first
      expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_time).to be_nil
      expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_1.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement_1.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement_1.value_source_value).to eq(@naaccr_item_value)

      condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept.concept_id).first
      measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept.concept_id).first
      expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_time).to be_nil
      expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_2.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement_2.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement_2.value_source_value).to eq(@naaccr_item_value)
    end

    it 'pointing to EPISODE', focus: false do
      expect(ConditionOccurrence.count).to eq(2)
      expect(Measurement.where(modifier_of_field_concept_id: 1000000003).count).to eq(2) #1000000003 = ‘episode.episode_id’ concept

      condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept.concept_id).first
      #1000000003 = ‘episode.episode_id’ concept
      measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept.concept_id).first
      expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_time).to be_nil
      expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
      expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_1.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement_1.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement_1.value_source_value).to eq(@naaccr_item_value)

      condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept.concept_id).first
      measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept.concept_id).first
      expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_time).to be_nil
      expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
      expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
      expect(measurement_2.measurement_source_value).to eq(@naaccr_item_number)
      expect(measurement_2.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
      expect(measurement_2.value_source_value).to eq(@naaccr_item_value)
    end
  end

  describe 'Ambiguous ICDO3 codes participating in multiple NAACCR schemas' do
    describe 'Creating entries in MEASUREMENT table for a standard categorical schema-independent diagnosis modifier' do
      before(:each) do
        @diagnosis_date = '20170630'
        @histology_site = '8013/3-C16.1'
        @naaccr_item_number = '1182'              #Lymph-vascular Invasion
        @naaccr_item_value = '1'                  #Lymph-vascular Invasion Present/Identified

        #390=Date of Diagnosis
        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_1.person_id \
          , record_id: '1' \
          , naaccr_item_number: '390' \
          , naaccr_item_value: @diagnosis_date \
          , histology: '8013/3' \
          , site: 'C16.1' \
          , histology_site: @histology_site \
        )

        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_1.person_id \
          , record_id: '1' \
          , naaccr_item_number: @naaccr_item_number \
          , naaccr_item_value: @naaccr_item_value  \
          , histology: '8013/3' \
          , site: 'C16.1' \
          , histology_site: @histology_site \
        )
        @measurement_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
        @measurement_source_concept = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
        @measurement_value_as_concept = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_item_number}@#{@naaccr_item_value}")
        @condition_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
        NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
      end

      it 'pointing to CONDITION_OCCURRENCE', focus: false do
        expect(Measurement.where(modifier_of_field_concept_id: 1147127).count).to eq(1)       #1147127 = 'condition_occurrence.condition_occurrence_id'
        measurement = Measurement.where(modifier_of_field_concept_id: 1147127).first
        expect(measurement.person_id).to eq(@person_1.person_id)
        expect(measurement.measurement_concept_id).to eq(@measurement_concept.concept_id)
        expect(measurement.measurement_date).to eq(Date.parse(@diagnosis_date))
        expect(measurement.measurement_time).to be_nil
        expect(measurement.measurement_datetime).to eq(Date.parse(@diagnosis_date))
        expect(measurement.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement.value_as_concept_id).to eq(@measurement_value_as_concept.concept_id)
        expect(measurement.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement.value_source_value).to eq(@naaccr_item_value)
        expect(ConditionOccurrence.count).to eq(1)
        condition_occurrence = ConditionOccurrence.first
        expect(measurement.modifier_of_event_id).to eq(condition_occurrence.condition_occurrence_id)
        expect(measurement.modifier_of_field_concept_id).to eq(1147127) #‘condition_occurrence.condition_occurrence_id’ concept
      end

      it 'pointing to EPISODE' do
        expect(Measurement.where(modifier_of_field_concept_id: 1000000003).count).to eq(1)
        measurement = Measurement.where(modifier_of_field_concept_id: 1000000003).first
        expect(measurement.person_id).to eq(@person_1.person_id)
        expect(measurement.measurement_concept_id).to eq(@measurement_concept.concept_id)
        expect(measurement.measurement_date).to eq(Date.parse(@diagnosis_date))
        expect(measurement.measurement_time).to be_nil
        expect(measurement.measurement_datetime).to eq(Date.parse(@diagnosis_date))
        expect(measurement.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement.value_as_concept_id).to eq(@measurement_value_as_concept.concept_id)
        expect(measurement.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement.value_source_value).to eq(@naaccr_item_value)
        expect(Episode.count).to eq(1)
        episode = Episode.first
        expect(measurement.modifier_of_event_id).to eq(episode.episode_id)
        expect(measurement.modifier_of_field_concept_id).to eq(1000000003) #‘‘episode.episode_id’ concept
      end
    end

    describe 'Creating entries in MEASUREMENT table for a standard categorical schema-dependent NAACCR value diagnosis modifier' do
      before(:each) do
        @histology = '8000/0'
        @site = 'C16.1'
        @histology_site = "#{@histology}-#{@site}"
        @diagnosis_date_1 = '20170630'
        @diagnosis_date_2 = '20180630'

        @naaccr_item_number = '772'               #EOD Primary Tumor
        @naaccr_item_value = '200'                #200

        @naaccr_item_number_discriminator = '2879'

        @naaccr_item_number_discriminator_1 = 'stomach@2879'              #Schema Discriminator: EsophagusGEJunction (EGJ)/Stomach
        @naaccr_schema_concept_code_1 = 'stomach'
        @naaccr_item_value_discriminator_1 = '030'                #030

        @naaccr_item_number_discriminator_2 = 'esophagus_gejunction@2879' #Schema Discriminator: EsophagusGEJunction (EGJ)/Stomach
        @naaccr_schema_concept_code_2 = 'esophagus_gejunction'
        @naaccr_item_value_discriminator_2 = '040'                #040

        #Person 1
        #390=Date of Diagnosis
        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_1.person_id \
          , record_id: '1' \
          , naaccr_item_number: '390' \
          , naaccr_item_value: @diagnosis_date_1 \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_1.person_id \
          , record_id: '1' \
          , naaccr_item_number: @naaccr_item_number \
          , naaccr_item_value: @naaccr_item_value  \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        #2879=CS SITE-SPECIFIC FACTOR25
        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_1.person_id \
          , record_id: '1' \
          , naaccr_item_number: @naaccr_item_number_discriminator  \
          , naaccr_item_value: @naaccr_item_value_discriminator_1  \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        #Person 2
        #390=Date of Diagnosis
        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_2.person_id \
          , record_id: '2' \
          , naaccr_item_number: '390' \
          , naaccr_item_value: @diagnosis_date_2 \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_2.person_id \
          , record_id: '2' \
          , naaccr_item_number: @naaccr_item_number \
          , naaccr_item_value: @naaccr_item_value  \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        #2879=CS SITE-SPECIFIC FACTOR25
        FactoryBot.create(:naaccr_data_point \
          , person_id: @person_2.person_id \
          , record_id: '2' \
          , naaccr_item_number: @naaccr_item_number_discriminator  \
          , naaccr_item_value: @naaccr_item_value_discriminator_2  \
          , histology: @histology \
          , site: @site \
          , histology_site: @histology_site \
        )

        @measurement_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)
        @measurement_source_concept = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number)

        @measurement_discriminator_concept_1 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number_discriminator_1)
        @measurement_discriminator_concept_2 =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number_discriminator_2)

        @measurement_discriminator_source_concept_1 = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number_discriminator_1)
        @measurement_discriminator_source_concept_2 = NaaccrEtl::SpecSetup.concept(vocabulary_id: 'NAACCR', concept_code: @naaccr_item_number_discriminator_2)

        @measurement_value_as_concept_1 = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_schema_concept_code_1}@#{@naaccr_item_number}@#{@naaccr_item_value}")
        @measurement_value_as_concept_2 = NaaccrEtl::SpecSetup.naaccr_value_concept(concept_code: "#{@naaccr_schema_concept_code_2}@#{@naaccr_item_number}@#{@naaccr_item_value}")

        @condition_concept =  NaaccrEtl::SpecSetup.standard_concept(vocabulary_id: 'ICDO3', concept_code: @histology_site)
        NaaccrEtl::Setup.execute_naaccr_etl(@legacy)
      end

      it 'pointing to CONDITION_OCCURRENCE', focus: false do
        expect(ConditionOccurrence.count).to eq(2)
        expect(Measurement.where(modifier_of_field_concept_id: 1147127).count).to eq(4) #1147127 = 'condition_occurrence.condition_occurrence_id'

        condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept.concept_id).first
        #1147127 = 'condition_occurrence.condition_occurrence_id'
        measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept_1.concept_id).first
        expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_1.measurement_time).to be_nil
        expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_1.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement_1.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement_1.value_source_value).to eq(@naaccr_item_value)

        measurement_3 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_discriminator_concept_1.concept_id).first
        expect(measurement_3.measurement_date).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_3.measurement_time).to be_nil
        expect(measurement_3.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_3.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_3.measurement_source_value).to eq(@naaccr_item_number_discriminator_1)
        expect(measurement_3.measurement_source_concept_id).to eq(@measurement_discriminator_source_concept_1.concept_id)
        expect(measurement_3.value_source_value).to eq(@naaccr_item_value_discriminator_1)

        condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept.concept_id).first
        measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept_2.concept_id).first
        expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_2.measurement_time).to be_nil
        expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_2.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement_2.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement_2.value_source_value).to eq(@naaccr_item_value)

        measurement_4 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1147127, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id,  measurement_concept_id: @measurement_discriminator_concept_2.concept_id).first
        expect(measurement_4.measurement_date).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_4.measurement_time).to be_nil
        expect(measurement_4.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_4.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_4.measurement_source_value).to eq(@naaccr_item_number_discriminator_2)
        expect(measurement_4.measurement_source_concept_id).to eq(@measurement_discriminator_source_concept_2.concept_id)
        expect(measurement_4.value_source_value).to eq(@naaccr_item_value_discriminator_2)
      end

      it 'pointing to EPISODE', focus: false do
        expect(ConditionOccurrence.count).to eq(2)
        expect(Measurement.where(modifier_of_field_concept_id: 1000000003).count).to eq(4) #1000000003 = ‘episode.episode_id’ concept

        condition_occurrence_1 = ConditionOccurrence.where(person_id: @person_1.person_id, condition_concept_id: @condition_concept.concept_id).first
        #1000000003 = ‘episode.episode_id’ concept
        measurement_1 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept_1.concept_id).first
        expect(measurement_1.measurement_date).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_1.measurement_time).to be_nil
        expect(measurement_1.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_1.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_1.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement_1.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement_1.value_source_value).to eq(@naaccr_item_value)

        measurement_3 = Measurement.where(person_id: @person_1.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_1.condition_occurrence_id,  measurement_concept_id: @measurement_discriminator_concept_1.concept_id).first
        expect(measurement_3.measurement_date).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_3.measurement_time).to be_nil
        expect(measurement_3.measurement_datetime).to eq(Date.parse(@diagnosis_date_1))
        expect(measurement_3.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_3.measurement_source_value).to eq(@naaccr_item_number_discriminator_1)
        expect(measurement_3.measurement_source_concept_id).to eq(@measurement_discriminator_source_concept_1.concept_id)
        expect(measurement_3.value_source_value).to eq(@naaccr_item_value_discriminator_1)

        condition_occurrence_2 = ConditionOccurrence.where(person_id: @person_2.person_id, condition_concept_id: @condition_concept.concept_id).first
        measurement_2 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id, measurement_concept_id: @measurement_concept.concept_id, value_as_concept_id: @measurement_value_as_concept_2.concept_id).first
        expect(measurement_2.measurement_date).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_2.measurement_time).to be_nil
        expect(measurement_2.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_2.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_2.measurement_source_value).to eq(@naaccr_item_number)
        expect(measurement_2.measurement_source_concept_id).to eq(@measurement_source_concept.concept_id)
        expect(measurement_2.value_source_value).to eq(@naaccr_item_value)

        measurement_4 = Measurement.where(person_id: @person_2.person_id, modifier_of_field_concept_id: 1000000003, modifier_of_event_id: condition_occurrence_2.condition_occurrence_id,  measurement_concept_id: @measurement_discriminator_concept_2.concept_id).first
        expect(measurement_4.measurement_date).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_4.measurement_time).to be_nil
        expect(measurement_4.measurement_datetime).to eq(Date.parse(@diagnosis_date_2))
        expect(measurement_4.measurement_type_concept_id).to eq(32534) # 32534 = ‘Tumor registry type concept
        expect(measurement_4.measurement_source_value).to eq(@naaccr_item_number_discriminator_2)
        expect(measurement_4.measurement_source_concept_id).to eq(@measurement_discriminator_source_concept_2.concept_id)
        expect(measurement_4.value_source_value).to eq(@naaccr_item_value_discriminator_2)
      end
    end
  end
end