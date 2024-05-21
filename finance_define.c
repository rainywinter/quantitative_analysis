/*
<: little endian
h: short(2 bytes)
H: unsigned short(2 bytes)
I: unsigned int(4 bytes)
L: unsigned long(4 bytes)
s: char[]
c: char (1 bytes)
f: float(4 bytes)
*/

// python header format: <hIH3L
// python chunk format: <6scL
// header example: (1, 20220331, 4831, 720896, 2324, 0)
struct tdx_cw {
    // header size: 20
    short unknown_header_1;          // 未明确，可能是版本号
    unsigned int date;               // Ymd格式的日期
    unsigned short report_num;       // 包含报告份数
    unsigned long unknown_header_2;  //
    unsigned long cw_chunk_bytes;    // cw块大小
    unsigned long unknown_header_3;  // 未明确

    // each share header size: 11
    // total size: 11* report_num
    struct share_header {
        char code[6];          // 股票代码
        char pad;              //填充
        unsigned long offset;  // cw信息偏移量
    } * share_headers;

    // size: report_num * cw_chunk_bytes
    float *cw_chunks[];
};

/*
写文件时按照struct结构顺序写入
*/

int main() {}