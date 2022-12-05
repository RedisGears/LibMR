extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use quote::format_ident;
use syn;

#[proc_macro_derive(BaseObject)]
pub fn base_object_derive(item: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(item).unwrap();
    let name = &ast.ident;

    let func_name = format_ident!("register_{}", name.to_string().to_lowercase());

    let gen = quote! {
        impl BaseObject for #name {
            fn get_name() -> &'static str {
                concat!(stringify!(#name), "\0")
            }
        }

        #[linkme::distributed_slice(mr::libmr::REGISTER_LIST)]
        fn #func_name() {
            #name::register();        
        }
    };

    gen.into()
}