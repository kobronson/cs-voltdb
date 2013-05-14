#include<string>
#include<set>

struct CsXML
{
    std::string m_file;          // log filename
    int m_level;                 // debug level
    std::set <std::string> m_modules;  // modules where logging is enabled
    void load(const std::string &filename);
    void save(const std::string &filename);
};
