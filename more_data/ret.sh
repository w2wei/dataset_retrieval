data_dir="../datamed_json"
name="wei"
gap=2
python arrayexpress.py $data_dir $name $gap &
python clinicaltrials.py $data_dir $name $gap &
python dryad.py $data_dir $name $gap &
python gemma.py $data_dir $name $gap &
python geo.py $data_dir $name $gap &
python mpd.py $data_dir $name $gap &
python neuromorpho.py $data_dir $name $gap &
python nursadatasets.py $data_dir $name $gap &
python proteomexchange.py $data_dir $name $gap &