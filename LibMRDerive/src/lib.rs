extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(BaseObject)]
pub fn base_object_derive(item: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(item).unwrap();
    let name = &ast.ident;

    let gen = quote! {
        impl BaseObject for #name {
            fn get_name() -> &'static str {
                concat!(stringify!(#name), "\0")
            }
        }
    };

    gen.into()
}