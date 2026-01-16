echo "Information for 'demo.surli':"
./surrealism info demo.surli

echo ""
echo "Signature for 'can_drive' in 'demo.surli':"
./surrealism sig --fnc can_drive demo.surli

echo ""
echo "Running 'can_drive' function with argument 17 in 'demo.surli':"
./surrealism run --fnc can_drive --arg 17 demo.surli

echo ""
echo "Running 'can_drive' function with argument 18 in 'demo.surli':"
./surrealism run --fnc can_drive --arg 18 demo.surli

echo ""
echo "Running 'result' function with argument false in 'demo.surli':"
./surrealism run --fnc result --arg false demo.surli

echo ""
echo "Running 'result' function with argument true in 'demo.surli':"
./surrealism run --fnc result --arg true demo.surli

echo ""
echo "Running 'test_kv' function in 'demo.surli':"
./surrealism run --fnc test_kv demo.surli

echo ""
echo "Running 'test_io' function in 'demo.surli':"
./surrealism run --fnc test_io demo.surli

echo ""
echo "Running 'test_none_value' function in 'demo.surli':"
./surrealism run --fnc test_none_value demo.surli