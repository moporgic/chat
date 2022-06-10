default:
	g++ -std=c++17 -O3 -march=native -Wall -fmessage-length=0 -o chat chat.cpp
	
static:
	g++ -std=c++17 -O3 -march=native -Wall -fmessage-length=0 -static -o chat chat.cpp
	upx chat || :
