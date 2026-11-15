材料提供Arrow C Data/C Stream ABI的真实本机生产者。编译：clang -std=c11 -shared -fPIC source.c -o libsource.dylib（Linux后缀可用.so）。source输出3个int64列批，每批5项，第2号项为NULL，值为批号*100+位置。生命周期计数只记录生产者实际分配/释放。
这不是目标consumer实现。scenario.json给一个确定交错；候选需实现可切片的所有权接管与多下游执行。允许Python ctypes、C或C++，可用成熟Arrow导入器，但须保留可检查地址与release证据。
